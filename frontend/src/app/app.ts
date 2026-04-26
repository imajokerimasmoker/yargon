import { Component, ElementRef, ViewChild, AfterViewInit, OnDestroy, ChangeDetectorRef } from '@angular/core';
import { WebsocketService, StreamMessage } from './websocket';
import { CommonModule } from '@angular/common';
import { DomSanitizer, SafeUrl } from '@angular/platform-browser';

interface RemoteStream {
  id: string;
  mediaSource: MediaSource;
  sourceBuffer: SourceBuffer | null;
  queue: Blob[];
  videoUrl: string;
  safeVideoUrl: SafeUrl;
  syncInterval?: any;
}

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './app.html',
  styleUrl: './app.css'
})
export class App implements AfterViewInit, OnDestroy {
  @ViewChild('localVideo') localVideo!: ElementRef<HTMLVideoElement>;

  private mediaRecorder: MediaRecorder | null = null;
  public remoteStreams: Map<string, RemoteStream> = new Map();

  constructor(
    private wsService: WebsocketService,
    private cdr: ChangeDetectorRef,
    private sanitizer: DomSanitizer
  ) {}

  ngAfterViewInit() {
    this.wsService.connect('ws://localhost:8080/ws').subscribe(msg => {
      this.handleRemoteStream(msg);
    });
  }

  async startStreaming() {
    try {
      const stream = await navigator.mediaDevices.getUserMedia({ video: true, audio: true });
      this.localVideo.nativeElement.srcObject = stream;

      const options = { mimeType: 'video/webm; codecs=vp8,opus' };
      this.mediaRecorder = new MediaRecorder(stream, options);

      this.mediaRecorder.ondataavailable = (event) => {
        if (event.data.size > 0) {
          this.wsService.sendMessage(event.data);
        }
      };

      this.mediaRecorder.start(100);
    } catch (err) {
      console.error('Error accessing media devices.', err);
    }
  }

  get remoteStreamList() {
    return Array.from(this.remoteStreams.values());
  }

  trackByFn(index: number, item: RemoteStream) {
    return item.id;
  }

  handleRemoteStream(msg: StreamMessage) {
    let remote = this.remoteStreams.get(msg.senderId);
    if (!remote) {
      const mediaSource = new MediaSource();
      const rawUrl = URL.createObjectURL(mediaSource);
      remote = {
        id: msg.senderId,
        mediaSource: mediaSource,
        sourceBuffer: null,
        queue: [],
        videoUrl: rawUrl,
        safeVideoUrl: this.sanitizer.bypassSecurityTrustUrl(rawUrl)
      };
      this.remoteStreams.set(msg.senderId, remote);

      mediaSource.addEventListener('sourceopen', () => {
        const sb = mediaSource.addSourceBuffer('video/webm; codecs=vp8,opus');
        remote!.sourceBuffer = sb;
        sb.addEventListener('updateend', () => {
          this.processQueue(remote!);
        });
        this.processQueue(remote!);
      });

      this.cdr.detectChanges();

      // Attempt to play once data starts coming in
      setTimeout(() => {
        const videoElement = document.querySelector(`video[data-stream-id="${remote!.id}"]`) as HTMLVideoElement;
        if (videoElement) {
          videoElement.muted = true; // Ensure it's muted

          const syncStream = () => {
            if (videoElement.paused && videoElement.readyState >= 1) {
              videoElement.play().catch(() => {});
            }

            if (videoElement.buffered.length > 0) {
              const lastIndex = videoElement.buffered.length - 1;
              const bufferedStart = videoElement.buffered.start(lastIndex);
              const bufferedEnd = videoElement.buffered.end(lastIndex);

              // If we are significantly behind the last buffered range or not in any range
              if (videoElement.currentTime < bufferedStart || videoElement.currentTime > bufferedEnd + 0.5) {
                console.log(`[${remote!.id}] Syncing: currentTime=${videoElement.currentTime.toFixed(3)}, buffered=[${bufferedStart.toFixed(3)}, ${bufferedEnd.toFixed(3)}]. Jumping to ${bufferedStart.toFixed(3)}`);
                videoElement.currentTime = bufferedStart;
              }
            }
          };

          videoElement.addEventListener('waiting', () => {
            console.log(`[${remote!.id}] Video waiting... readyState=${videoElement.readyState}, currentTime=${videoElement.currentTime.toFixed(3)}`);
            syncStream();
          });

          videoElement.addEventListener('loadedmetadata', () => {
            videoElement.play().catch(() => {});
          });

          // Persistent interval for synchronization
          const playInterval = setInterval(syncStream, 1000);

          if (remote) {
            remote.syncInterval = playInterval;
          }
        }
      }, 500);
    }

    this.pushToBuffer(remote, msg.data);
  }

  async pushToBuffer(remote: RemoteStream, blob: Blob) {
    remote.queue.push(blob);
    this.processQueue(remote);
  }

  async processQueue(remote: RemoteStream) {
    if (remote.queue.length > 0 && remote.sourceBuffer && !remote.sourceBuffer.updating && remote.mediaSource.readyState === 'open') {
      const blob = remote.queue.shift()!;
      try {
        const arrayBuffer = await blob.arrayBuffer();
        remote.sourceBuffer.appendBuffer(arrayBuffer);
      } catch (e) {
        console.error('Error processing queue', e);
      }
    }
  }

  ngOnDestroy() {
    if (this.mediaRecorder) {
      this.mediaRecorder.stop();
    }
    this.remoteStreams.forEach(remote => {
      if (remote.syncInterval) {
        clearInterval(remote.syncInterval);
      }
    });
    this.wsService.close();
  }
}
