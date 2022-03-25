#include"csapp.h"
#include<stdbool.h>

/* Recommended max cache and object sizes */
#define MAX_CACHE_SIZE 1049000
#define MAX_OBJECT_SIZE 102400
#define MAX_HDR_CNT 512

enum request_error_type{REQ_OK, REQ_MALFORMED, REQ_UNIMPLEMENTED}; // All cases of errors that may occured parsing request line
enum header_error_type{HDR_OK, HDR_MALFORMED, HDR_UNIMPLEMENTED};
enum entity_error_type{ENT_OK, ENT_MALFORMED};
enum client_error_type{CLIENT_ERR_400, CLIENT_ERR_500, CLIENT_ERR_501}; // All cases of errors that might be sent to client
struct request_info {
	enum request_error_type err_type;
	char *method, *host, *port, *abs_path, *http_version;
};
struct header_info {
	enum header_error_type err_type;
	int count;
	bool has_entity_body;
	char *(*kvpairs)[2];
};
struct entity_info {
	enum entity_error_type err_type;
	char *data;
};

/* You won't lose style points for including this long line in your code */
static const char *user_agent_hdr = "User-Agent: Mozilla/5.0 (X11; Linux x86_64; rv:10.0.3) Gecko/20120305 Firefox/10.0.3\r\n";

void serve(int clientfd);

struct request_info parse_request(rio_t *);
struct header_info parse_header(rio_t *);
struct entity_info parse_entity(rio_t *);

struct request_info convert_client_to_server_request(struct request_info);
struct header_info convert_client_to_server_header(struct header_info, struct request_info);

int send_request(int, struct request_info);
int send_header(int, struct header_info);
int send_entity(int, struct entity_info);

int connect_to_server(struct request_info);
int forward_server_to_client(rio_t *, rio_t *);

void clienterror(int fd, enum client_error_type);

int main(int argc, char *argv[])
{
	int listenfd, connfd;
	struct sockaddr_storage sockaddr;
	socklen_t len;
	if(argc != 2)
	{
		fprintf(stderr, "\e[1;031mUsage: %s port\n", argv[0]);
		exit(1);
	}
	listenfd = Open_listenfd(argv[1]);
	while(1)
	{
		connfd = Accept(listenfd, (SA *)&sockaddr, &len);
		if(fork() == 0)
		{
			serve(connfd);
			Close(connfd);
			exit(0);
		}
	}
	printf("%s", user_agent_hdr);
	return 0;
}

/**
 * @brief serve the given client until it is finished.
 * 
 * @param clientfd the file descriptor of client
 */
void serve(int clientfd)
{
	struct request_info client_req_info, server_req_info;
	struct header_info client_hdr_info, server_hdr_info;
	struct entity_info ent_info;
	int serverfd;
	rio_t rio_client, rio_server;

	rio_readinitb(&rio_client, clientfd);
	client_req_info = parse_request(&rio_client);
	switch(client_req_info.err_type)
	{
		case REQ_OK:
			break;

		case REQ_MALFORMED:
			clienterror(clientfd, CLIENT_ERR_400);
			goto end;
		
		case REQ_UNIMPLEMENTED:
			clienterror(clientfd, CLIENT_ERR_501);
			goto end;

		default:
			clienterror(clientfd, CLIENT_ERR_500);
			goto end;
	}
	if((serverfd = connect_to_server(client_req_info)) == -1)
	{
		goto end;
	}
	server_req_info = convert_client_to_server_request(client_req_info);
	rio_readinitb(&rio_server, serverfd);
	if(send_request(serverfd, server_req_info))
	{
		goto end;
	}
	client_hdr_info = parse_header(&rio_client);
	switch(client_hdr_info.err_type)
	{
		case REQ_OK:
			break;

		case REQ_MALFORMED:
			clienterror(clientfd, CLIENT_ERR_400);
			goto end;
		
		case REQ_UNIMPLEMENTED:
			clienterror(clientfd, CLIENT_ERR_501);
			goto end;

		default:
			clienterror(clientfd, CLIENT_ERR_500);
			goto end;
	}
	server_hdr_info = convert_client_to_server_header(client_hdr_info, client_req_info);
	send_header(serverfd, server_hdr_info);

	if(client_hdr_info.has_entity_body)
	{
		goto end;
	}
	
	ent_info = parse_entity(&rio_client);
	switch(ent_info.err_type)
	{
		case ENT_MALFORMED:
			clienterror(clientfd, CLIENT_ERR_400);
			goto end;
		
		case ENT_OK:
			break;
		
		default:
			clienterror(clientfd, CLIENT_ERR_500);
			goto end;
	}
	send_entity(serverfd, ent_info);
	forward_server_to_client(&rio_server, &rio_client);

	end: return;
}

/**
 * @brief read and parse request line from the given rio_t
 * 
 * @param rio initialized rio_t of the file descriptor
 * @return struct request_info - details of request line
 */
struct request_info parse_request(rio_t *rio)
{
	struct request_info ret;
	char str[MAXLINE], method[MAXLINE], uri[MAXLINE], http_version[MAXLINE], host[MAXLINE], port[MAXLINE], path[MAXLINE], *pos;
	int read_cnt;
 
	if(rio_readlineb(rio, str, MAXLINE) == 0)
	{
		ret.err_type = REQ_MALFORMED;
		goto end;
	}
	/* Parse request line into 3 parts roughly */
	if (sscanf(str, "%s%s%s %n", method, uri, http_version, &read_cnt) < 3 || read_cnt != strlen(str))
	{
		ret.err_type = REQ_MALFORMED;
		goto end;
	}
 
#ifndef SUPPORT_POST
	if(strcmp(method, "POST") == 0)
	{
		ret.err_type = REQ_UNIMPLEMENTED;
		goto end;
	}
#endif

	/* parse uri part */
	for (size_t i = 0, len = strlen(uri); i < len; i++) // convert all capital to lowercase
		uri[i] = tolower(uri[i]);
	if ((sscanf(uri, "http://%[^:/]%n", host, &read_cnt)) < 1) // read leading scheme and host
	{
		ret.err_type = REQ_MALFORMED;
		goto end;
	}
	pos = uri + read_cnt;     // update position pointer
	if (uri[read_cnt] == ':') // if port is specified, parse port
	{
		sscanf(pos, ":%[^/]%n", port, &read_cnt);
		pos += read_cnt;                 // update position
	}
	else // else use default port 80
	{
		strcpy(port, "80");
	}
	if (sscanf(pos, "%s", path) < 1) // read path
	{
		ret.err_type = REQ_MALFORMED;
		goto end;
	}
 
	/* construct the return struct */
	ret.err_type = REQ_OK;
	ret.method = strdup(method);
	ret.host = strdup(host);
	ret.port = strdup(port);
	ret.abs_path = strdup(path);
	ret.http_version = strdup(http_version);
 
end:
	return ret;
}

/**
 * @brief read and parse all headers from the given rio_t
 * 
 * @param rio initialized rio_t of the file descriptor
 * @return struct header_info - details of request line
 */
struct header_info parse_header(rio_t *rio)
{
	struct header_info ret;
	char buf[MAXLINE], key[MAXLINE], val[MAXLINE], *(kvpairs[MAX_HDR_CNT])[2];
	int readcnt;

	ret.count = 0;
	while(rio_readlineb(rio, buf, MAXLINE))
	{
		if(strcmp(buf, "\r\n") == 0)
			break;
		if(sscanf(buf, "%[^:]: %s %n", key, val, &readcnt) < 2 || readcnt != strlen(buf))
		{
			ret.err_type = HDR_MALFORMED;
			goto end;
		}
		kvpairs[ret.count][0] = strdup(key);
		kvpairs[ret.count][1] = strdup(val);
		ret.count++;
	}
	ret.err_type = HDR_OK;
	ret.has_entity_body = false;
	if(ret.count)
	{
		ret.kvpairs = Malloc(sizeof(*ret.kvpairs) * --ret.count);
		memcpy(ret.kvpairs, kvpairs, sizeof(*ret.kvpairs) * ret.count);
		for(int i = 0; i < ret.count; i++)
		{
			ret.kvpairs[i][0] = strdup(kvpairs[i][0]);
			ret.kvpairs[i][1] = strdup(kvpairs[i][1]);
		}
	}
	else
	{
		ret.kvpairs = NULL;
	}

	end: return ret;
}

struct entity_info parse_entity(rio_t *rio)
{
	struct entity_info ret;
	ret.err_type = ENT_OK;
	return ret;
}
/**
 * @brief convert client request line struct to server request line struct
 * 
 * @param in client request line
 * @return struct request_info - server request line, can be directly passed to send_request
 */
struct request_info convert_client_to_server_request(struct request_info in)
{
	struct request_info ret;

	ret.err_type = in.err_type;
	ret.method = strdup(in.method);
	ret.host = NULL;
	ret.port = NULL;
	ret.abs_path = strdup(in.abs_path);
	ret.http_version = strdup(in.http_version);

	return ret;
}

/**
 * @brief convert client header struct to server header struct
 * 
 * @param in client header
 * @param req_info client request line
 * @return struct header_info - server header_info, can be directly passed to send_header
 */
struct header_info convert_client_to_server_header(struct header_info in, struct request_info req_info)
{
	struct header_info ret;

	ret.err_type = in.err_type;
	ret.count = in.count + 4; // add 4 additional header first, and subtract them if needed.
	ret.has_entity_body = in.has_entity_body;

	ret.kvpairs = Malloc((in.count + 4) * sizeof(*ret.kvpairs));

	ret.kvpairs[0][0] = strdup("Host");
	ret.kvpairs[0][1] = strdup(req_info.host);
	ret.kvpairs[1][0] = strdup("User-Agent");
	ret.kvpairs[1][1] = strdup("Mozilla/5.0 (X11; Linux x86_64; rv:10.0.3) Gecko/20120305 Firefox/10.0.3");
	ret.kvpairs[2][0] = strdup("Connection");
	ret.kvpairs[2][1] = strdup("close");
	ret.kvpairs[3][0] = strdup("Proxy-Connection");
	ret.kvpairs[3][1] = strdup("close");
	for(int i = 0, j = 4; i < in.count; i++)
	{
		if(strcmp(in.kvpairs[i][0], "Host") == 0)
		{
			ret.count--;
			continue;
		}
		else if(strcmp(in.kvpairs[i][0], "User-Agent") == 0)
		{
			ret.count--;
			continue;
		}
		else if(strcmp(in.kvpairs[i][0], "Connection") == 0)
		{
			ret.count--;
			continue;
		}
		else if(strcmp(in.kvpairs[i][0], "Proxy-Connection") == 0)
		{
			ret.count--;
			continue;
		}
		else
		{
			ret.kvpairs[j][0] = strdup(in.kvpairs[i][0]);
			ret.kvpairs[j][1] = strdup(in.kvpairs[i][1]);
			j++;
		}
	}

	return ret;
}

/**
 * @brief send request line to server
 * 
 * @param fd server fd
 * @param in server request_info
 * @return int - state
 */
int send_request(int fd, struct request_info in)
{
	dprintf(fd, "%s %s %s\r\n", in.method, in.abs_path, in.http_version);
	return 0;
}

/**
 * @brief send headers to server
 * 
 * @param fd server fd
 * @param in server request_info
 * @return int - state
 */
int send_header(int fd, struct header_info in)
{
	for(int i = 0; i < in.count; i++)
		dprintf(fd, "%s: %s\r\n", in.kvpairs[i][0], in.kvpairs[i][1]);
	dprintf(fd, "\r\n");
	return 0;
}

/**
 * @brief send entity to server
 * 
 * @param fd server fd
 * @param in server request_info
 * @return int - state
 */
int send_entity(int fd, struct entity_info in)
{
	return 0;
}

/**
 * @brief connect to the server in the parameter
 * 
 * @param in server request info
 * @return int - serverfd or -1 if failed
 */
int connect_to_server(struct request_info in)
{
	return open_clientfd(in.host, in.port);
}

/**
 * @brief forward all output of server to client
 * 
 * @param rio_server server rio_t
 * @param rio_client client rio_t
 * @return int unused
 */
int forward_server_to_client(rio_t *rio_server, rio_t *rio_client)
{
	char buf[MAXLINE];
	ssize_t read_cnt;
	while((read_cnt = rio_readnb(rio_server, buf, MAXLINE)) > 0)
	{
		rio_writen(rio_client->rio_fd, buf, read_cnt);
	}
	return 0;
}

/**
 * @brief output error page to client
 * 
 * @param fd client fd
 * @param err_type enum value of err_type
 */
void clienterror(int fd, enum client_error_type err_type)
{
	switch(err_type)
	{
		case CLIENT_ERR_400:
			dprintf(fd, "HTTP/1.0 400 Bad Request\r\n\r\n");
			break;
		
		case CLIENT_ERR_501:
			dprintf(fd, "HTTP/1.0 501 Not Implemented\r\n\r\n");
			break;

		case CLIENT_ERR_500:
		default:
			dprintf(fd, "HTTP/1.0 500 Bad Request\r\n\r\n");
			break;
	}
}