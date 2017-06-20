// http://beej.us/guide/bgnet/output/html/singlepage/bgnet.html#theory

// needed for GNU extension to recvfrom: recvmmsg, bswap
#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <byteswap.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>


#define PACKHEADER 114                   // Size of the packet header = PACKETSIZE-PAYLOADSIZE in bytes

#define PACKETSIZE_STOKESI  6364         // Size of the packet, including the header in bytes
#define PAYLOADSIZE_STOKESI 6250         // Size of the record = packet - header in bytes

#define PACKETSIZE_STOKESIQUV  8114      // Size of the packet, including the header in bytes
#define PAYLOADSIZE_STOKESIQUV 8000      // Size of the record = packet - header in bytes
#define PAYLOADSIZE_MAX        8000      // Maximum of payload size of I, IQUV

#define MMSG_VLEN  256            // Batch message into single syscal using recvmmsg()

#define TIMEUNIT 781250           // Conversion factor of timestamp from seconds to (1.28 us) packets
#define UMSPPACKET (1.0)          // sleep time in microseconds between sending two packets

/*
 * Header description based on:
 * ARTS Interface Specification from BF to SC3+4
 * ASTRON_SP_066_InterfaceSpecificationSC34.pdf
 * revision 2.0
 */
typedef struct {
  unsigned char marker_byte;         // See table 3 in PDF, page 6
  unsigned char format_version;      // Version: 1
  unsigned char cb_index;            // [0,36] one compound beam per fill_ringbuffer instance:: ignore
  unsigned char tab_index;           // [0,11] all tabs per fill_ringbuffer instance
  unsigned short channel_index;      // [0,1535] all channels per fill_ringbuffer instance
  unsigned short payload_size;       // Stokes I: 6250, IQUV: 8000
  unsigned long timestamp;           // units of 1.28 us, since 1970-01-01 00:00.000 
  unsigned char sequence_number;     // SC3: Stokes I: 0-1, Stokes IQUV: 0-24
                                     // SC4: Stokes I: 0-3, Stokes IQUV: 0-49
  unsigned char reserved[7];
  unsigned long flags[3];
  unsigned char record[PAYLOADSIZE_MAX];
} packet_t;

int main(int argc , char *argv[]) {
  int sockfd;
  struct addrinfo hints, *servinfo, *p;

  memset(&hints, 0, sizeof hints);
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_DGRAM;

  char service[256];
  snprintf(service, 255, "%i", 7469);

  // find possible connections
  if(getaddrinfo("127.0.0.1", service, &hints, &servinfo) != 0) {
    perror(NULL);
    exit(EXIT_FAILURE);
  }

  // loop through all the results and make a socket
  for(p = servinfo; p != NULL; p = p->ai_next) {
    sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
    if (sockfd == -1) {
      perror("talker: socket");
      continue;
    }

    if (connect(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
      close(sockfd);
      continue;
    }

    break;
  }

  if (p==NULL) {
    fprintf(stderr, "Cannot open connection\n");
    exit(EXIT_FAILURE);
  }

  // keep sending packets till user hits C-c
  packet_t packet_buffer[MMSG_VLEN];   // Buffer for batch requesting packets via recvmmsg
  unsigned int packet_idx;             // Current packet index in MMSG buffer
  struct iovec iov[MMSG_VLEN];         // IO vec structure for recvmmsg
  struct mmsghdr msgs[MMSG_VLEN];      // multimessage hearders for recvmmsg

  // multi message setup
  memset(msgs, 0, sizeof(msgs));
  for(packet_idx=0; packet_idx < MMSG_VLEN; packet_idx++) {
    iov[packet_idx].iov_base = (char *) &packet_buffer[packet_idx];
    iov[packet_idx].iov_len = PACKETSIZE_STOKESI;

    msgs[packet_idx].msg_hdr.msg_name    = NULL; // we don't need to know who sent the data
    msgs[packet_idx].msg_hdr.msg_iov     = &iov[packet_idx];
    msgs[packet_idx].msg_hdr.msg_iovlen  = 1;
    msgs[packet_idx].msg_hdr.msg_control = NULL; // we're not interested in OoB data
  }

  packet_t *packet;                // Pointer to current packet
  int counter = 0;
  unsigned long curr_packet;       // Current timestamp
  unsigned long dropped = 0;       // deliberately dropped packets
  while(1) {
    for(packet_idx=0; packet_idx < MMSG_VLEN; packet_idx++) {
      packet = &packet_buffer[packet_idx];

      packet->marker_byte = 0xE0; // case 4 mode 0
      packet->format_version = 1;
      packet->cb_index = 1;
      packet->payload_size = bswap_16(PAYLOADSIZE_STOKESI);

      packet->sequence_number = counter % 4;
      packet->tab_index = (counter / 4) % 12;
      packet->channel_index = bswap_16((counter / (4 * 12)) % 1536);

      curr_packet = TIMEUNIT * (counter / (4 * 12 * 1536));
      packet->timestamp = bswap_64(curr_packet);

      //if (counter % 12345 == 0) {
      //  // deliberately drop some packets
      //  counter += 1;
      //  dropped++;
      //}

      counter += 1;
    }

    // Send some data
    // res = sendto(sockfd, &packet, PACKETSIZE, 0, p->ai_addr, p->ai_addrlen);
    if (sendmmsg(sockfd, msgs, MMSG_VLEN, 0) == -1) {
      perror("ERROR Could not send packets");
      goto exit;
    }

    // slow down sending a bit
    usleep(UMSPPACKET  * 0.5);
  }

exit:
  fprintf(stderr, "Deliberately unsent packets:    %li\n", dropped);
  // done, clean up
  free(servinfo); 
  close(sockfd);
}
