#ifndef CLOUDSCRIBE_INETADDR_H
#define CLOUDSCRIBE_INETADDR_H

#ifdef __cplusplus
extern "C" {
#endif


int get_ip_by_eth(const char *ethname, char *buf,size_t bufsize);


#ifdef __cplusplus
}
#endif


#endif /* CLOUDSCRIBE_INETADDR_H */
