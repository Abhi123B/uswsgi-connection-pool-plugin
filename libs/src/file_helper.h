#ifndef __FILE_HELPER_H__
#define __FILE_HELPER_H__

#include <vector>
#include <string>

/* this funtion is used to get the RMQ connection strings from the config file in non DB mode
 * it assumes the config varibles in config file are comma separated
 * @param   fileName    the config file path
 * @param   conn_str_id the config key specifying RMQ connection string
 * @param   vec         a string vector containing the result
 * @example RMQ_CONN_STR    id,cnt_str_primary,cnt_str_secondary
 */
void get_rmq_conn_str(const char* fileName, const char* conn_str_id, std::vector<std::string> *vec);

#endif