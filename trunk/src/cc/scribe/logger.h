#ifndef CLOUDSCRIBE_LOGGER_H
#define CLOUDSCRIBE_LOGGER_H



/*
 * Debug logging
 */
#define LOG_OPER(format_string,...)                                             \
{                                                                               \
        time_t now;                                                             \
        char dbgtime[26] ;                                                      \
        time(&now);                                                             \
        ctime_r(&now, dbgtime);                                                 \
        dbgtime[24] = '\0';                                                     \
        fprintf(stderr,"[%s] " #format_string " \n", dbgtime,##__VA_ARGS__);    \
}



#endif /* CLOUDSCRIBE_LOGGER_H */
