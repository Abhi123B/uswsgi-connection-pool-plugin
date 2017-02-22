#include "parser_defs.h"
#include <string.h>
#include <fstream>

void parse_body(char* knowlusId, char* queueName, char* msg, const char *body, int len)
{
    int i = 0, flag =0, j=0, k=0, l=0;
    
    for(i=0;i < len;i++)
    {
        if(body[i] == '|') {
                flag++;
        }
        else {        
            if(flag == 0)
            {
                knowlusId[j] = body[i];
                j++;
            }
            else if(flag == 1)
            {
                queueName[l] = body[i];
                l++;
            }
            else {
                msg[k] = body[i];
                k++;
            }
        }
    }
    
    knowlusId[j] = '\0';
    queueName[l] = '\0';
    msg[k] = '\0';
}

int parse_line(const char *line, int len, char **_id, char **str1, char **str2)
{
    int pos1 = 0 ,pos2 = 0, pos3 = 0;
    int i = 0, cnt = 0, l = 0;
    char delim = '|';
    int ret = 0;
    
    for(i=0;i<len;i++)
    {
        if(line[i] == '#')
            break;
        if(line[i] == delim || i == (len-1)){
            cnt++;
            switch(cnt)
            {
                case 1: pos1 = i-1;
                        break;
                case 2: pos2 = i-1;
                        break;
                default: pos3 = i;
            }
        }
        if(cnt >= 3)
            break;
    }
    
    if(pos1>=0 && pos2>0)
    {
        l = pos1+1;
        *_id = new char[l+1];
        memcpy(*_id, line, l);
        *(*_id+l) = '\0';
    
        l = pos2 - (pos1 + 1);
        *str1 = new char[l+1];
        memcpy(*str1, line+pos1+2, l);
        *(*str1+l) = '\0';
        
        ret = 1;
        
        if(pos3 > 0)
        {
            l = pos3 - (pos2 + 1);
            *str2 = new char[l+1];
            memcpy(*str2, line+pos2+2, l);
            *(*str2+l) = '\0';
        }
        else
        {
            *str2 = NULL;
        }
    }
    
    return ret;
}

int parse_rmq_ip_string_db(const char *line, int len, char delim, int pos, char* buffer)
{
    int i = 0, j = 0, flag = 0;
    char ip = '.' , port = ':';
    int ip_cnt = 0, port_cnt = 0;
    
    for(i=0;i<len;i++)
    {
        if(line[i]==delim)
        {
            flag++;
            if(flag == pos)
            {
                ip_cnt = 0;
                port_cnt = 0;
            }
        }
        else if(flag>pos)
        {
            break;
        }
        else
        {
            if(flag == pos)
            {
                buffer[j] = line[i];
                j++;
            }
            
            if(line[i] == ip) {
                ip_cnt++;
            }
            else if(line[i] == port) {
                port_cnt++;
            }
        }    
    }
    
    buffer[j] = '\0';
    return (ip_cnt == 3 && port_cnt == 1) ? j: 0;
}

int parse_rmq_con_str(const char *line, int len, char *ibuf , char* cbuf ,char delim)
{
    int i=0, j=0 ,k =0;
    int flag = 0;
    
    for(i=0;i<len;i++)
    {
        if(line[i]==delim)
        {
            flag++;
        }
        else
        {
            if(flag == 0)
            {
                ibuf[j] = line[i];
                j++;
            }
            else if(flag == 1)
            {
                cbuf[k] = line[i];
                k++; 
            }
        }    
    }
    
    ibuf[j] = '\0';
    cbuf[k] = '\0';
    
    return ((j+k) > k) ? j : 0;
}

int getConfig(const char *fileName , const char *key, char* str)
{
    std::string line;
    std::fstream ifile;
    int found = 0, i =0, j=0, k=0, flag = 0;
    
    ifile.open(fileName , std::ios::in);
    if(!ifile.is_open())
    {
        fprintf(stderr,"unable to open file : %s\n", fileName);
        return -1;
    }
    
    while(getline(ifile, line) && !found)
    {
        flag = 1;
        
        if(line[0] == '#')
            continue;

		for(i=0;key[i]!='\0';i++)
		{
			if(key[i] != line[i]) {
				flag = 0;
				break;
			}
		}
        
        if(flag)
		{
			found = 1;
			k=0;
			for(j=i+1;line[j]!='\0';j++)
			{
					str[k] = line[j];
					k++;
			}
			str[k] = '\0';
		}
    }
    
    ifile.close();
    
    return found;
}