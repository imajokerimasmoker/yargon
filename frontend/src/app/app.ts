import { Component, ElementRef, ViewChild, AfterViewInit, OnDestroy, ChangeDetectorRef } from '@angular/core';
import { WebsocketService, StreamMessage } from './websocket';
import { CommonModule } from '@angular/common';

interface RemoteStream {
  id: string;
  mediaSource: MediaSource;
  sourceBuffer: SourceBuffer | null;
  queue: Blob[];
  videoUrl: string;
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

  constructor(private wsService: WebsocketService, private cdr: ChangeDetectorRef) {}

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

  handleRemoteStream(msg: StreamMessage) {
    let remote = this.remoteStreams.get(msg.senderId);
    if (!remote) {
      const mediaSource = new MediaSource();
      remote = {
        id: msg.senderId,
        mediaSource: mediaSource,
        sourceBuffer: null,
        queue: [],
        videoUrl: URL.createObjectURL(mediaSource)
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
        const videoElement = document.querySelector(`video[src="${remote!.videoUrl}"]`) as HTMLVideoElement;
        if (videoElement) {
          videoElement.muted = true; // Ensure it's muted
          videoElement.play().catch(err => console.log("Autoplay failed, waiting for user interaction:", err));
        }
      }, 100);
    }

    this.pushToBuffer(remote, msg.data);
  }

  async pushToBuffer(remote: RemoteStream, blob: Blob) {
    if (remote.sourceBuffer && !remote.sourceBuffer.updating && remote.mediaSource.readyState === 'open') {
      try {
        const arrayBuffer = await blob.arrayBuffer();
        remote.sourceBuffer.appendBuffer(arrayBuffer);
      } catch (e) {
        console.error('Error appending buffer', e);
      }
    } else {
      remote.queue.push(blob);
    }
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
    this.wsService.close();
  }
}
