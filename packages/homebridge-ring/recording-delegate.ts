import type { RingCamera } from 'ring-client-api'
import type {
  CameraRecordingConfiguration,
  CameraRecordingDelegate,
  HDSProtocolSpecificErrorReason,
  RecordingPacket,
} from 'homebridge'
import { logDebug, logError, logInfo } from 'ring-client-api/util'
import { reservePorts } from '@homebridge/camera-utils'
import { getFfmpegPath } from 'ring-client-api/ffmpeg'
import type { StreamingSession } from 'ring-client-api/streaming/streaming-session'
import { firstValueFrom, Subject } from 'rxjs'
import { take, takeUntil } from 'rxjs/operators'
import type { ChildProcess, SpawnOptions } from 'child_process'
import { spawn } from 'child_process'
import { createServer } from 'net'
import type { Server, Socket } from 'net'

interface ActiveRecordingStream {
  streamId: number
  session: StreamingSession
  ffmpegProcess?: ChildProcess
  generator?: AsyncGenerator<RecordingPacket>
  closeRequested: boolean
  dataSubject: Subject<Buffer>
  stopSignal: Subject<void>
}

export class RingRecordingDelegate implements CameraRecordingDelegate {
  private recordingActive = false
  private recordingConfiguration?: CameraRecordingConfiguration
  private prebufferSession?: StreamingSession
  private activeStream?: ActiveRecordingStream
  private prebufferData: Buffer[] = []
  private readonly ringCamera: RingCamera

  constructor(ringCamera: RingCamera) {
    this.ringCamera = ringCamera
  }

  updateRecordingActive(active: boolean): void {
    logInfo(
      `Recording ${active ? 'enabled' : 'disabled'} for ${
        this.ringCamera.name
      }`,
    )
    this.recordingActive = active

    if (active && this.recordingConfiguration) {
      this.startPrebuffering().catch(logError)
    } else {
      this.stopPrebuffering()
    }
  }

  updateRecordingConfiguration(
    configuration: CameraRecordingConfiguration | undefined,
  ): void {
    logDebug(
      `Recording configuration ${configuration ? 'updated' : 'cleared'} for ${
        this.ringCamera.name
      }`,
    )
    this.recordingConfiguration = configuration

    if (this.recordingActive && configuration) {
      // Restart prebuffering with new configuration
      this.stopPrebuffering()
      this.startPrebuffering().catch(logError)
    }
  }

  async *handleRecordingStreamRequest(
    streamId: number,
  ): AsyncGenerator<RecordingPacket> {
    const configuration = this.recordingConfiguration
    if (!configuration) {
      logError(
        `No recording configuration available for ${this.ringCamera.name}`,
      )
      return
    }

    logInfo(`Starting recording stream ${streamId} for ${this.ringCamera.name}`)

    const dataSubject = new Subject<Buffer>(),
      stopSignal = new Subject<void>()

    this.activeStream = {
      streamId,
      session: undefined as unknown as StreamingSession,
      closeRequested: false,
      dataSubject,
      stopSignal,
    }

    try {
      // Start a new live call for recording
      const session = await this.ringCamera.startLiveCall(),
        fragmentLength =
          configuration.mediaContainerConfiguration.fragmentLength,
        resolution = configuration.videoCodec.resolution,
        videoBitrate = configuration.videoCodec.parameters.bitRate,
        audioBitrate = configuration.audioCodec.bitrate,
        audioSamplerate = this.getAudioSamplerate(
          configuration.audioCodec.samplerate,
        ),
        // Reserve ports for RTP streams
        [videoPort] = await reservePorts({ count: 1 }),
        [audioPort] = await reservePorts({ count: 1 }),
        // Create a TCP server to receive fragmented MP4 data from ffmpeg
        { server, port: outputPort } = await this.createTcpServer(dataSubject),
        // Wait for the call to be answered to get the SDP
        ringSdpResult = await Promise.race([
          // eslint-disable-next-line dot-notation
          firstValueFrom(session['connection'].onCallAnswered) as Promise<
            string | void
          >,
          firstValueFrom(session.onCallEnded.pipe(take(1))).then(
            () => undefined,
          ),
        ])

      this.activeStream.session = session

      if (!ringSdpResult || typeof ringSdpResult !== 'string') {
        logError(`Call ended before answered for ${this.ringCamera.name}`)
        server.close()
        return
      }

      const ringSdp = ringSdpResult,
        // Get the input SDP for ffmpeg
        inputSdp = this.createInputSdp(ringSdp, videoPort, audioPort),
        // Build ffmpeg arguments for fragmented MP4 output
        ffmpegArgs = this.buildFfmpegArgs({
          resolution,
          videoBitrate,
          audioBitrate,
          audioSamplerate,
          fragmentLength,
          outputPort,
          iFrameInterval: configuration.videoCodec.parameters.iFrameInterval,
        }),
        ffmpegPath = getFfmpegPath() ?? 'ffmpeg',
        ffmpeg: ChildProcess = spawn(ffmpegPath, ffmpegArgs, {
          stdio: ['pipe', 'pipe', 'pipe'],
        } as SpawnOptions)

      logDebug(`Starting ffmpeg for HKSV recording: ${ffmpegArgs.join(' ')}`)

      this.activeStream.ffmpegProcess = ffmpeg

      // Pipe the SDP to ffmpeg stdin
      if (ffmpeg.stdin) {
        ffmpeg.stdin.write(inputSdp)
        ffmpeg.stdin.end()
      }

      if (ffmpeg.stderr) {
        ffmpeg.stderr.on('data', (data: Buffer) => {
          logDebug(`HKSV ffmpeg: ${data.toString()}`)
        })
      }

      ffmpeg.on('error', (error: Error) => {
        logError(`HKSV ffmpeg error for ${this.ringCamera.name}: ${error}`)
        stopSignal.next()
      })

      ffmpeg.on('exit', (code: number | null) => {
        logDebug(
          `HKSV ffmpeg exited with code ${code} for ${this.ringCamera.name}`,
        )
        stopSignal.next()
        server.close()
      })

      // Forward RTP packets from the Ring stream to ffmpeg
      // eslint-disable-next-line dot-notation
      const videoSplitter = session['videoSplitter'],
        // eslint-disable-next-line dot-notation
        audioSplitter = session['audioSplitter']

      session.addSubscriptions(
        session.onVideoRtp.pipe(takeUntil(stopSignal)).subscribe((rtp) => {
          videoSplitter
            .send(rtp.serialize(), { port: videoPort })
            .catch(logError)
        }),
        session.onAudioRtp.pipe(takeUntil(stopSignal)).subscribe((rtp) => {
          audioSplitter
            .send(rtp.serialize(), { port: audioPort })
            .catch(logError)
        }),
      )

      // Track if we've sent the media initialization
      let sentMediaInit = false,
        currentFragment = Buffer.alloc(0),
        moovBox: Buffer | undefined

      // Process fragmented MP4 data from ffmpeg
      for await (const chunk of this.createAsyncIterator(
        dataSubject,
        stopSignal,
      )) {
        if (this.activeStream?.closeRequested) {
          break
        }

        // Accumulate data and parse MP4 boxes
        currentFragment = Buffer.concat([currentFragment, chunk])

        // Parse and yield complete MP4 boxes
        while (currentFragment.length >= 8) {
          const boxSize = currentFragment.readUInt32BE(0),
            boxType = currentFragment.subarray(4, 8).toString('ascii')

          if (currentFragment.length < boxSize) {
            // Wait for more data
            break
          }

          const box = currentFragment.subarray(0, boxSize)
          currentFragment = currentFragment.subarray(boxSize)

          if (boxType === 'ftyp' || boxType === 'moov') {
            // Media initialization boxes
            if (boxType === 'moov') {
              moovBox = box
            }
            if (!sentMediaInit && moovBox) {
              // Yield ftyp + moov as media initialization
              const ftypBox = this.createFtypBox()
              yield {
                data: Buffer.concat([ftypBox, moovBox]),
                isLast: false,
              }
              sentMediaInit = true
            }
          } else if (boxType === 'moof' || boxType === 'mdat') {
            // Media fragment boxes - moof followed by mdat
            if (boxType === 'moof') {
              // Start of a new fragment, wait for mdat
              const fragmentData = [box]

              // Look for the mdat box
              while (currentFragment.length >= 8) {
                const nextBoxSize = currentFragment.readUInt32BE(0),
                  nextBoxType = currentFragment.subarray(4, 8).toString('ascii')

                if (currentFragment.length < nextBoxSize) {
                  break
                }

                const nextBox = currentFragment.subarray(0, nextBoxSize)
                currentFragment = currentFragment.subarray(nextBoxSize)
                fragmentData.push(nextBox)

                if (nextBoxType === 'mdat') {
                  // Complete fragment
                  yield {
                    data: Buffer.concat(fragmentData),
                    isLast: false,
                  }
                  break
                }
              }
            }
          }
        }
      }

      // Signal end of stream
      if (!this.activeStream?.closeRequested) {
        yield {
          data: Buffer.alloc(0),
          isLast: true,
        }
      }
    } catch (error) {
      logError(`Recording stream error for ${this.ringCamera.name}: ${error}`)
    } finally {
      this.cleanupActiveStream()
    }
  }

  acknowledgeStream(streamId: number): void {
    logDebug(`Stream ${streamId} acknowledged for ${this.ringCamera.name}`)
  }

  closeRecordingStream(
    streamId: number,
    reason: HDSProtocolSpecificErrorReason | undefined,
  ): void {
    logInfo(
      `Closing recording stream ${streamId} for ${
        this.ringCamera.name
      }, reason: ${reason ?? 'connection closed'}`,
    )

    if (this.activeStream?.streamId === streamId) {
      this.activeStream.closeRequested = true
      this.activeStream.stopSignal.next()
      this.cleanupActiveStream()
    }
  }

  private startPrebuffering(): Promise<void> {
    if (this.prebufferSession) {
      return Promise.resolve()
    }

    logDebug(`Starting prebuffer for ${this.ringCamera.name}`)

    // Note: For a full implementation, we would maintain a rolling buffer
    // of the last N seconds of video. For now, we'll start fresh on each
    // recording event since Ring cameras have their own cloud recording.
    this.prebufferData = []
    return Promise.resolve()
  }

  private stopPrebuffering(): void {
    if (this.prebufferSession) {
      logDebug(`Stopping prebuffer for ${this.ringCamera.name}`)
      this.prebufferSession.stop()
      this.prebufferSession = undefined
    }
    this.prebufferData = []
  }

  private cleanupActiveStream(): void {
    if (this.activeStream) {
      this.activeStream.stopSignal.next()
      this.activeStream.stopSignal.complete()
      this.activeStream.dataSubject.complete()

      if (this.activeStream.ffmpegProcess) {
        this.activeStream.ffmpegProcess.kill('SIGTERM')
      }

      if (this.activeStream.session) {
        this.activeStream.session.stop()
      }

      this.activeStream = undefined
    }
  }

  private createTcpServer(
    dataSubject: Subject<Buffer>,
  ): Promise<{ server: Server; port: number }> {
    return new Promise((resolve, reject) => {
      const server = createServer((socket: Socket) => {
        socket.on('data', (data) => {
          dataSubject.next(data)
        })
        socket.on('error', (error) => {
          logError(`TCP server socket error: ${error}`)
        })
      })

      server.on('error', reject)
      server.listen(0, '127.0.0.1', () => {
        const address = server.address()
        if (address && typeof address === 'object') {
          resolve({ server, port: address.port })
        } else {
          reject(new Error('Failed to get server address'))
        }
      })
    })
  }

  private createInputSdp(
    ringSdp: string,
    videoPort: number,
    audioPort: number,
  ): string {
    // Clean up the SDP and update ports
    return ringSdp
      .split('\n')
      .filter((line) => !line.startsWith('a=rtcp-mux'))
      .join('\n')
      .replace(/m=audio \d+/, `m=audio ${audioPort}`)
      .replace(/m=video \d+/, `m=video ${videoPort}`)
  }

  private buildFfmpegArgs(options: {
    resolution: [number, number, number]
    videoBitrate: number
    audioBitrate: number
    audioSamplerate: number
    fragmentLength: number
    outputPort: number
    iFrameInterval: number
  }): string[] {
    const {
        resolution,
        videoBitrate,
        audioBitrate,
        audioSamplerate,
        fragmentLength,
        outputPort,
        iFrameInterval,
      } = options,
      [width, height, fps] = resolution,
      gopSize = Math.round((fps * iFrameInterval) / 1000)

    return [
      '-hide_banner',
      '-protocol_whitelist',
      'pipe,udp,rtp,file,crypto',
      '-f',
      'sdp',
      '-i',
      'pipe:',
      // Video encoding
      '-vcodec',
      'libx264',
      '-pix_fmt',
      'yuv420p',
      '-profile:v',
      'high',
      '-level:v',
      '4.0',
      '-b:v',
      `${videoBitrate}k`,
      '-bufsize',
      `${videoBitrate * 2}k`,
      '-maxrate',
      `${videoBitrate * 1.5}k`,
      '-r',
      fps.toString(),
      '-g',
      gopSize.toString(),
      '-keyint_min',
      gopSize.toString(),
      '-sc_threshold',
      '0',
      '-vf',
      `scale=${width}:${height}`,
      // Audio encoding (AAC-LC for HKSV)
      '-acodec',
      'aac',
      '-b:a',
      `${audioBitrate}k`,
      '-ar',
      audioSamplerate.toString(),
      '-ac',
      '1',
      // Fragmented MP4 output
      '-f',
      'mp4',
      '-movflags',
      'frag_keyframe+empty_moov+default_base_moof',
      '-frag_duration',
      (fragmentLength * 1000).toString(),
      `tcp://127.0.0.1:${outputPort}`,
    ]
  }

  private getAudioSamplerate(samplerate: number): number {
    // Map AudioRecordingSamplerate enum values to actual sample rates
    const samplerates: Record<number, number> = {
      0: 8000,
      1: 16000,
      2: 24000,
      3: 32000,
      4: 44100,
      5: 48000,
    }
    return samplerates[samplerate] ?? 24000
  }

  private createFtypBox(): Buffer {
    // Create a minimal ftyp box for fragmented MP4
    const ftyp = Buffer.alloc(24)
    ftyp.writeUInt32BE(24, 0)
    ftyp.write('ftyp', 4)
    ftyp.write('isom', 8)
    ftyp.writeUInt32BE(0x200, 12)
    ftyp.write('isomiso2', 16)
    return ftyp
  }

  private async *createAsyncIterator(
    dataSubject: Subject<Buffer>,
    stopSignal: Subject<void>,
  ): AsyncGenerator<Buffer> {
    const queue: Buffer[] = [],
      resolverRef: { current: (() => void) | null } = { current: null }
    let done = false

    const subscription = dataSubject.subscribe({
        next: (data) => {
          queue.push(data)
          if (resolverRef.current) {
            resolverRef.current()
            resolverRef.current = null
          }
        },
        complete: () => {
          done = true
          if (resolverRef.current) {
            resolverRef.current()
            resolverRef.current = null
          }
        },
      }),
      stopSubscription = stopSignal.pipe(take(1)).subscribe(() => {
        done = true
        if (resolverRef.current) {
          resolverRef.current()
          resolverRef.current = null
        }
      })

    try {
      while (!done || queue.length > 0) {
        if (queue.length > 0) {
          yield queue.shift()!
        } else if (!done) {
          await new Promise<void>((r) => {
            resolverRef.current = r
          })
        }
      }
    } finally {
      subscription.unsubscribe()
      stopSubscription.unsubscribe()
    }
  }

  destroy(): void {
    this.stopPrebuffering()
    this.cleanupActiveStream()
  }
}
