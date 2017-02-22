#ifndef __ERROR_DEFS_H__
#define __ERROR_DEFS_H__

#include "uws_pg_config.h"
#include <pthread.h>

class uwsError
{
    int                        lastError;
    char                       lastMsg[UWS_MAXX_ERROR_MSG_SIZE];
    pthread_mutex_t            lock;

public:
    uwsError();
    virtual ~uwsError();
  
    virtual void               setLastError(int errno);
    virtual void               setLastMsg(const char* msg);
    virtual const char*        getLastErrorMsg(void);
    virtual int                getLastErrorCode(void);
};

#endif