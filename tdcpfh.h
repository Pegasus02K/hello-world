/* ----------------------------- tcobfh.h ----------------------------------- */
/*                                                                            */
/*                  Copyright (c) 2006 Tmax Soft Co., Ltd                     */
/*                        All Rights Reserved                                 */
/*                                                                            */
/*   THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF Tmax Soft.                */
/*   The copyright notice above does not evidence any actual or               */
/*   intended publication of such source code.                                */
/*                                                                            */
/* -------------------------------------------------------------------------- */

#ifndef __TDCPFH_H_
#define __TDCPFH_H_

#include <inttypes.h>

#if defined (__cplusplus)
extern "C" {
#endif

/* -------------------------- open mode ------------------------------------- */

#define TDCPFH_OPEN_INPUT				0
#define TDCPFH_OPEN_OUTPUT			1

/* -------------------------- file I/O block -------------------------------- */

// !!!!DCPCXCB. 
// file access: sequential
// file organization: sequential "PS"
// record format: fixed"FB(F)" (but later can be variable"VB"?)
typedef struct tdcpcxcb_s
{
    char			file_name[8]; // DDNAME		PIC X(08)  VALUE  DD-NAME. : No extra space for '\0' in file_name!
    uint16_t		lrecl;    // LRECL			PIC 9(04)  COMP  VALUE 0. : logical record length
    uint16_t		chkpt;    // CHKPT			PIC 9(04)  COMP  VALUE 0. : checkpoint
    char			copyc;    // COPYC		PIC X(01)  VALUE ' Y'.
    char			monoc;    // MONOC		PIC X(01)  VALUE  'Y'.
    char			ioabend;    // IOABEND		PIC X(01)  VALUE  'Y'.
    char 			lrecfm;    // LRECFM		PIC X(01)  VALUE  'F'. : logical record format
    // SYSTEM		PIC X(112). : 108 bytes for extra information
    uint32_t		file_handle;    // 4 bytes for file handle
    char			return_code[4];    // 4 bytes for return code. "    " for success, "EOF " for eof, "ERR " for error
    uint8_t		open_mode;    // 1 byte for saving open mode set from tdcpfh_open
    uint8_t		is_open;    // 1 byte for saving internal open indicator
    uint8_t		is_read;    // 1 byte for saving internal read indicator 
    char			filler[101];    // extra filler space: 112 - (4+4+1+1+1=11)
} tdcpcxcb_t;

/* -------------------------- file open session ----------------------------- */

extern int OPENCXD(tdcpcxcb_t *file, char *open_mode);
extern int CLOSECX(tdcpcxcb_t *file);

/* -------------------------- record access --------------------------------- */

extern int READXD(tdcpcxcb_t *file, char *buf, int buflen);
extern int CWRITED(tdcpcxcb_t *file, char *buf, int buflen);

/* -------------------------- end of header --------------------------------- */

#if defined (__cplusplus)
}
#endif

#endif /* __TDCPFH_H_ */
