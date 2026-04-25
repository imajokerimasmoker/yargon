import { Injectable } from '@angular/core';
import { Subject, Observable } from 'rxjs';

export interface StreamMessage {
  senderId: string;
  data: Blob;
}

@Injectable({
  providedIn: 'root'
})
export class WebsocketService {
  private socket: WebSocket | null = null;
  private messageSubject = new Subject<StreamMessage>();

  constructor() {}

  public connect(url: string): Observable<StreamMessage> {
    this.socket = new WebSocket(url);
    this.socket.binaryType = 'arraybuffer';

    this.socket.onmessage = async (event) => {
      if (event.data instanceof ArrayBuffer) {
        const view = new DataView(event.data);
        const idLength = view.getUint8(0);
        const decoder = new TextDecoder();
        const senderId = decoder.decode(new Uint8Array(event.data, 1, idLength));
        const data = event.data.slice(1 + idLength);

        this.messageSubject.next({
          senderId,
          data: new Blob([data], { type: 'video/webm; codecs=vp8,opus' })
        });
      }
    };

    this.socket.onerror = (event) => {
      console.error('WebSocket error:', event);
    };

    this.socket.onclose = (event) => {
      console.log('WebSocket connection closed:', event);
    };

    return this.messageSubject.asObservable();
  }

  public sendMessage(data: Blob | ArrayBuffer) {
    if (this.socket && this.socket.readyState === WebSocket.OPEN) {
      this.socket.send(data);
    }
  }

  public close() {
    if (this.socket) {
      this.socket.close();
    }
  }
}
