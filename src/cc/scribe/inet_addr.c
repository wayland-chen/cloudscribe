#include <string.h>
#include <net/if.h>
#include <linux/sockios.h>
#include <arpa/inet.h>



int get_ip_by_eth(const char *ethname, char *buf,size_t bufsize) {
        in_addr_t addr = 0;
        int fd = socket(AF_INET, SOCK_DGRAM, 0);
        if(fd >= 0) {
                struct ifreq ir;
                bzero(&ir, sizeof(ir));
                strncpy(ir.ifr_name, ethname?ethname:"lo", 8);
                if(ioctl(fd, SIOCGIFADDR, &ir)==0)
                        addr = ((struct sockaddr_in *)&ir.ifr_addr)->sin_addr.s_addr;
                close(fd);
        }
        struct in_addr temp;
        temp.s_addr = addr;
        char *a = inet_ntoa(temp);
        if(strlen(a) > bufsize) {
                return -2;
        }
        strncpy(buf, inet_ntoa(temp), strlen(a));
        return strlen(a);
}
