#ifndef __PARSER_DEFS_H__
#define __PARSER_DEFS_H__
/*Parser utilities*/


/* This parses the raw byte request to get the connection id, 
 * the RMQ queuename and the message respectively, asumes comma seperated string
 */
int parse_line(const char *line, int len, char **_id, char **str1, char **str2);
/* does the same as above , but in this case buffers are not pre allocated*/
void parse_body(char* knowlusId, char* queueName, char* msg, const char *body, int len);
/* This parses the raw line string to get the RMQ string at position specified by 
 * @param pos, assumes comma seperated string */
int parse_rmq_ip_string_db(const char *line, int len, char delim, int pos, char* buffer);
/* This parses the raw line string  to get the connection id and string */
int parse_rmq_con_str(const char *line, int len, char *ibuf , char* cbuf, char delim);
/* Get the config vale defined by @param key from the config file defined by @param fileName*/
int getConfig(const char *fileName , const char *key, char* str);

#endif