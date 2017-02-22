#include "file_helper.h"
#include "uws_pg_config.h"
#include <fstream>

void get_rmq_conn_str(const char* fileName, const char* conn_str_id, std::vector<std::string> *vec)
{
    char connstr[UWS_MEDIUM_BUFFER_SIZE], _id[UWS_SMALL_BUFFER_SIZE];
    char str[UWS_LARGE_BUFFER_SIZE];
    std::string line;
    std::fstream ifile;
    int i=0, j=0, k=0, flag = 0, pos = 0;
    
    if(!fileName || !conn_str_id || !vec)
        return;
    
    ifile.open(fileName , std::ios::in);
    if(!ifile.is_open())
    {
        fprintf(stderr,"unable to open file : %s\n", fileName);
        return;
    }
    
    while(getline(ifile, line))
    {
        flag = 1;
        
        if(line[0] == '#')
            continue;

		for(i=0;conn_str_id[i]!='\0';i++)
		{
			if(conn_str_id[i] != line[i]) {
				flag = 0;
				break;
			}
		}
        
        k = 0;
        if(flag)
        {
            for(j=i+1;line[j]!=',';j++)
            {
                if(line[j]=='\0') {
                    flag = 0;
                    break;
                }
                _id[k++] = line[j];
            }
            _id[k] = '\0';
        }
        
        while(flag) {
            flag = 0;
			k=0;
			for(j=j+1;line[j]!='\0';j++)
			{
                if(line[j] == ',') {
                    flag = 1;
                    break;
                }
                connstr[k] = line[j];
                k++;
			}
			connstr[k] = '\0';
            sprintf(str,"%s%d,%s", _id, pos, connstr);
            vec->push_back(str);
            pos++;
		}
    }
    
    ifile.close();
}