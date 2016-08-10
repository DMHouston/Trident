/* 
 * udpserver.c - A simple UDP echo server 
 * usage: udpserver <port>
 */

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <netdb.h>
#include <sys/types.h> 
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pqxx/pqxx> 
using namespace std;
using namespace pqxx;
#define BUFSIZE 65536
#define MAX_DATA_LENGTH 256
#define BATTERY_DIVISOR 3920.92f


struct databaseInfo { //struct for sql commmand
    double battery;
    char *date;
    char *time;
    char *cdom;
    char *fluorescein;
    char *tribidity;
    char *depth;
    char *temp;
	  int errors;
};


/*
 * error - wrapper for perror
 */
void error(char *msg) {
  perror(msg);
  exit(1);
}



int main(int argc, char **argv) {
  FILE *data;

  int sockfd; /* socket */
  int portno; /* port to listen on */
  unsigned int clientlen; /* byte size of client's address */
  struct sockaddr_in serveraddr; /* server's addr */
  struct sockaddr_in clientaddr; /* client addr */
  struct hostent *hostp; /* client host info */
  char buf[BUFSIZE]; /* message buf */
  char *hostaddrp; /* dotted decimal host addr string */
  int optval; /* flag value for setsockopt */
  int n; /* message byte size */
  int fileCount = 0;
  /* 
   * check command line arguments 
   */
  if (argc != 2) {
    fprintf(stderr, "usage: %s <port>\n", argv[0]);
    exit(1);
  }
  portno = atoi(argv[1]);

  /* 
   * socket: create the parent socket 
   */
  sockfd = socket(AF_INET, SOCK_DGRAM, 0);
  if (sockfd < 0) 
    error((char *)"ERROR opening socket");

  /* setsockopt: Handy debugging trick that lets 
   * us rerun the server immediately after we kill it; 
   * otherwise we have to wait about 20 secs. 
   * Eliminates "ERROR on binding: Address already in use" error. 
   */
  optval = 1;
  setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, 
	     (const void *)&optval , sizeof(int));

  /*
   * build the server's Internet address
   */
  bzero((char *) &serveraddr, sizeof(serveraddr));
  serveraddr.sin_family = AF_INET;
  serveraddr.sin_addr.s_addr = htonl(INADDR_ANY);
  serveraddr.sin_port = htons((unsigned short)portno);

  /* 
   * bind: associate the parent socket with a port 
   */
  if (bind(sockfd, (struct sockaddr *) &serveraddr, 
	   sizeof(serveraddr)) < 0) 
    error((char *)"ERROR on binding");

  /* 
   * main loop: wait for a datagram, then echo it
   */
  clientlen = sizeof(clientaddr);
  data = fopen("c6data.txt", "a");
  fprintf(data,"Battery - Date - Time - CDOM/FDOM - Fluorescien - Turbidity - Depth - Temp\n");
  fclose(data);
  while (1) { //recvfrom: receive a UDP datagram from a client

    bzero(buf, BUFSIZE);
    n = recvfrom(sockfd, buf, BUFSIZE, MSG_OOB,
		 (struct sockaddr *) &clientaddr, &clientlen);
    
    if (n < 0)
      error((char *)"ERROR in recvfrom");

    /* 
     * gethostbyaddr: determine who sent the datagram
     */
    hostp = gethostbyaddr((const char *)&clientaddr.sin_addr.s_addr, 
			  sizeof(clientaddr.sin_addr.s_addr), AF_INET);
    if (hostp == NULL)
      error((char *)"ERROR on gethostbyaddr");
    hostaddrp = inet_ntoa(clientaddr.sin_addr);
    if (hostaddrp == NULL)
      error((char *)"ERROR on inet_ntoa\n");
    printf("server received datagram from %s (%s)\n", 
	   hostp->h_name, hostaddrp);
    printf("server received: %lu/%d bytes\n %s\n", strlen(buf), n, buf);
    data = fopen("c6data.txt", "a");
    fprintf(data,"%s", buf);
    fclose(data);
    
    data = fopen("workspace.txt", "w");
    fprintf(data,"%s", buf);
    fclose(data);
	data = fopen ("workspace.txt", "rb");

	char* buffer = NULL;
	size_t len;
	ssize_t bytes_read = getdelim( &buffer, &len, '\0', data);
	printf("Buffer of size %li was read into buffer : \n %s \n", bytes_read, buffer);
	if (buffer &&  bytes_read != -1)
	{
		// start to process your data / extract strings here...
		char const *start = "insert into data.statistics values('";
		char const *between = "', '";
		char const *end = "');";
		char *token;
		const char s[3] = " \n"; 
		struct databaseInfo information;
		double battery_level;
		char const *dateMarker = "/";
		char const *newLineMarker = "\n";
		bool once = true;
		int i;
		token = strtok(buffer, s);

		while (token != NULL){
			if(strstr(token, newLineMarker) != NULL) {
				token = strtok(NULL, s);
				i=0;
			}
			if(strstr(token, dateMarker) != NULL) { //only seen in date
				i = 1;
			}
			switch(i) {
				case 0:
					information.battery = atof(token);
					//printf( "battery is %f but should be %f.\n", information.battery, atof(token));
					information.battery = information.battery / BATTERY_DIVISOR;
					//printf( "battery level after divisor is : %f.\n", information.battery);					break;
				case 1:
					information.date = token;
					printf( "date is:%s.\n", information.date);
					break;
				case 2:
					information.time = token;
					//printf( "time is:%s.\n", information.time);
					break;
				case 3:
					information.cdom = token;
					printf( "cdom is:%s.\n", information.cdom);
					break;
				case 4:
					information.fluorescein = token;
					//printf( "fluorescein is:%s.\n", information.fluorescein);
					break;
				case 5:
					information.tribidity = token;
					//printf( "tribidity is:%s.\n", information.tribidity);
					break;
				case 6:
					information.depth = token;
					//printf( "depth is:%s.\n", information.depth);
					break;
				case 7:{
					information.temp = token;
					//printf( "Temp is:%s.\n", information.temp);

					char sql[1024];
					char const *start = "insert into data.statistics (battery, date,time,cdomfdom,fluorescein,tribidity,depth,temperature) values('";
					char const *between = "', '";
					char const *end = "');";
					sprintf (sql, "%s%f%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s\n",start, information.battery, 
					between,information.date, between, information.time, between, information.cdom, between, information.fluorescein,
					between, information.tribidity, between, information.depth, between, information.temp, end);
					printf("The sequel command is recieved as : %s \n", sql);
						//Sync data to database
					try{
						connection C("dbname=mydb user=labuser password=labuser \
						hostaddr=127.0.0.1 port=5432");
						if (C.is_open()) {
						printf("Opened database successfully: %s\n", C.dbname());
						} else {
						printf("Can't open database");
						return 1;
						}
						work W(C);        // Create a transactional object.
						W.exec( sql );  // Prepare the command
						W.commit();     // Execute SQL query
						printf( "Records created successfully\n");
						C.disconnect ();
						}catch (const std::exception &e){
						printf("%s\n", e.what());
						return 1;
						}
					}
					i=-1;
					break;
				default :
					break;
			} // end switch
			token = strtok(NULL, s);
			i++;
		} // end while (token != NULL)
	} // end if (buffer &&  bytes_read != -1) 
  

  
    //Rsync the data to the web server
    system("rsync --timeout=1 -e ssh c6data.txt ptueller@youngbulltrident.asu.edu:/var/www/users/youngbulltrident.asu.edu/data.txt");
    fileCount++;
    if (fileCount == 10) {
      fileCount = 0;
      system("mv ./c6data.txt ../");
      data = fopen("c6data.txt", "a");
      fprintf(data,"Battery - Date - Time - CDOM/FDOM - Fluorescien - Turbidity - Depth - Temp\n");
      fclose(data);
    }
  }
} // end main
