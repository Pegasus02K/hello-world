#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <usrinc/atmi.h>
#include <usrinc/fbuf.h>
#include <usrinc/tmaxapi.h>
#include <oframe_fdl.h>
#include "ofcom.h"
#include "ds.h"
#include "dscom.h"
#include "nvsm.h"


extern char *gdgdelete_version;
extern char *gdgdelete_build_info;


char gdgdelete_gdgname[DS_DSNAME_LEN + 2] = {0,};
char gdgdelete_catalog[DS_DSNAME_LEN + 2] = {0,};

int gdgdelete_force = 0;


int error_return(int error_code, char *function_name);
int system_error(char *function_name);

int check_args(int argc, char *argv[]);
int print_usage();

int validate_param();

int init_libraries();

int log_a_record(char *title, char *record, int rcode);

static void _signal_handler(int signo)
{
	switch(signo) {
		case SIGINT:
			printf("Caught SIGINT!\n");
			dscom_tool_logout();
			break;
		case SIGTERM:
			printf("Caught SIGTERM!\n");
			dscom_tool_logout();
			break;
		default:
			fprintf(stderr, "Unexpected signal!\n");
			exit(EXIT_FAILURE);
	}
	exit(EXIT_SUCCESS);
}

int main(int argc, char *argv[])
{
	int retval; char record[1024];
	/* TPCALL variables */
	FBUF *snd_buf = NULL;
	FBUF *rcv_buf = NULL;
	long rcv_len;
	char retmsg[1024];

	/* print version and one line summary */
	if( argc >= 2 && ! strcmp(argv[1], "-V") ) {
		printf("gdgdelete version %s %s\n", gdgdelete_version+26, gdgdelete_build_info+22);
		fflush(stdout); return 0;
	} else {
		printf("gdgdelete version %s %s\n", gdgdelete_version+26, gdgdelete_build_info+22);
		printf("Delete an Existing GDG (Generation Data Group)\n\n"); fflush(stdout);
	}

	/* check input arguments */
	retval = check_args(argc, argv);
	if( retval < 0 ) { print_usage(); return -1; }

	/* validate parameters */
	retval = validate_param();
	if( retval < 0 ) goto _GDGDELETE_MAIN_ERR_RETURN_00;

	/* compose trace log record */
	sprintf(record, "GDGNAME=%s,CATALOG=%s,FORCE=%s",
		gdgdelete_gdgname, gdgdelete_catalog, gdgdelete_force ? "YES" : "NO");

	/* print a log message */
	printf("GDGDELETE %s\n", record); fflush(stdout);
	retval = 0;

	/* tool login process */
	retval = dscom_tool_login("GDGDELETE");
	if( retval < 0 ) goto _GDGDELETE_MAIN_ERR_RETURN_01;


	/* catch signals */
	if (signal(SIGINT, _signal_handler) == SIG_ERR)
	{
		fprintf(stderr, "Cannot handle SIGINT!\n");
		exit(EXIT_FAILURE);
	}
	if (signal(SIGTERM, _signal_handler) == SIG_ERR)
	{
		fprintf(stderr, "Cannot handle SIGTERM!\n");
		exit(EXIT_FAILURE);
	}
	
	/* initialize user return code */
	tpurcode = 0;
	
	/* allocate send buffer */
	snd_buf = (FBUF *)tpalloc( "FIELD", NULL, 0);
	if(snd_buf == NULL)
	{
		fprintf(stderr, "gdgdelete: ***Couldn't allocate send buffer->%s\n",tpstrerror(tperrno)); 
		goto _GDGDELETE_MAIN_ERR_RETURN_02;
	}
	
	/* allocate receive buffer */
	rcv_buf = (FBUF *)tpalloc("FIELD", NULL, 0);
	if(rcv_buf == NULL)
	{
		fprintf(stderr, "gdgdelete: ***Couldn't allocate send buffer->%s\n",tpstrerror(tperrno)); 
		tpfree((char *)snd_buf);
		goto _GDGDELETE_MAIN_ERR_RETURN_02;
	}
	
	/* fbput FB_DSNAME */
	retval = fbput(snd_buf, FB_DSNAME, gdgdelete_gdgname, 0);
	if (retval == -1)
	{
		fprintf(stderr, "gdgdelete: ***An error occurred while storing DSNAME in field buffer->%s\n", fbstrerror(fberror)); 
		goto _GDGDELETE_MAIN_ERR_RETURN_03;
	}

	/* fbput FB_CATNAME */
	retval = fbput(snd_buf, FB_CATNAME, gdgdelete_catalog, 0);
	if (retval == -1)
	{
		fprintf(stderr, "gdgdelete: ***An error occurred while storing CATNAME in field buffer->%s\n", fbstrerror(fberror)); 
		goto _GDGDELETE_MAIN_ERR_RETURN_03;
	}
	
// force option should be specified here
	/* fbput FB_TYPE */
	if (gdgdelete_force)
		retval = fbput(snd_buf, FB_TYPE, "F", 0);
	if (retval == -1)
	{
		fprintf(stderr, "gdgdelete: ***An error occurred while storing TYPE in field buffer->%s\n", fbstrerror(fberror)); 
		goto _GDGDELETE_MAIN_ERR_RETURN_03;
	}
	
	/* tmax service call */
	retval = tpcall("OFRUISVRGDGDEL", (char *)snd_buf, 0, (char **)&rcv_buf, &rcv_len, TPNOFLAGS);
	if (retval < 0) 
	{	// an error occurred
		fprintf(stderr, "gdgcreate: ***An error occurred in server OFRUISVRGDGDEL->%s\n", tpstrerror(tperrno));
		retval = fbget(rcv_buf, FB_RETMSG, retmsg, 0);		
		fprintf(stderr, "----------------Return message----------------\n%s\n----------------------------------------------\n", retmsg);
		goto _GDGDELETE_MAIN_ERR_RETURN_03;
	}
	
	/* print a log message */
	printf("COMPLETED SUCCESSFULLY.\n"); fflush(stdout);
	retval = 0;
	
_GDGDELETE_MAIN_ERR_RETURN_03:
	/* release buffers */
	tpfree((char *)rcv_buf);
	tpfree((char *)snd_buf);

_GDGDELETE_MAIN_ERR_RETURN_02:
	/* tool logout process */
	dscom_tool_logout();

_GDGDELETE_MAIN_ERR_RETURN_01:
	/* call a function to log a record */
	log_a_record("GDGDELETE", record, retval);

_GDGDELETE_MAIN_ERR_RETURN_00:
	/* process returns here */
	return retval;
}


int error_return(int error_code, char *function_name)
{
	fprintf(stderr, "gdgdelete: *** %s failed - errcode=%d\n", function_name, error_code);
	return error_code;
}


int system_error(char *function_name)
{
	fprintf(stderr, "gdgdelete: *** %s failed - error=%s\n", function_name, strerror(errno));
	return -1;
}


#define SKIP_ARGI_OR_RETURN_ERROR(IDX, ARGC) { \
	if( (IDX)+1 < (ARGC) ) { \
		(IDX)++; \
	} else { \
		return -1; \
	} \
}


int check_args(int argc, char *argv[])
{
	int i;

	/* check command options */
	for( i = 1; i < argc; i++ ) {
		/* check if -c option is specified */
		if( ! strcmp(argv[i], "-c") || ! strcmp(argv[i], "--catalog") ) {
			SKIP_ARGI_OR_RETURN_ERROR(i, argc);
			if( strlen(argv[i]) > DS_DSNAME_LEN ) return -1;
			strcpy(gdgdelete_catalog, argv[i]);

		/* check if -F option is specified */
		} else if( ! strcmp(argv[i], "-F") || ! strcmp(argv[i], "--force") ) {
			gdgdelete_force = 1;

		/* otherwise <gdgname> specified */
		} else {
			if( argv[i][0] == '-' ) return -1;
			if( strlen(argv[i]) > DS_DSNAME_LEN - 9 ) return -1;

			if( gdgdelete_gdgname[0] ) return -1;
			strcpy(gdgdelete_gdgname, argv[i]);
		}
	}

	/* check if the GDG name is not specified */
	if( ! gdgdelete_gdgname[0] ) return -1;

	return 0;
}


int print_usage()
{
	printf("Usage: gdgdelete [options] <gdgname>\n");
	printf("<gdgname>           Specifies the GDG name to be deleted\n");
	printf("\n");

	printf("Options:\n");
	printf("  -c, --catalog     Specifies the catalog name that contains the GDG entry\n");
	printf("  -F, --force       Scratches all generation datasets pointed by the GDG base\n");
	printf("\n");

	return 0;
}


int validate_param()
{
	/* check if GDG name is specified */
	if( gdgdelete_gdgname[0] != '\0' ) {
		/* check if GDG name is invalid */
		if( dscom_check_dsname(gdgdelete_gdgname) < 0 ) {
			fprintf(stderr, "gdgdelete: *** invalid GDG name specified - gdgname=%s\n", gdgdelete_gdgname);
			return -1;
		}
	}

	/* check if catalog name is specified */
	if( gdgdelete_catalog[0] != '\0' ) {
		/* check if catalog name is invalid */
		if( dscom_check_dsname(gdgdelete_catalog) < 0 ) {
			fprintf(stderr, "gdgdelete: *** invalid catalog name specified - catalog=%s\n", gdgdelete_catalog);
			return -1;
		}
	}

	return 0;
}

int log_a_record(char *title, char *record, int rcode)
{
	int retval, length; char buffer[4096], sysdate[16], systime[16], fpath[256];

	strcpy(buffer, record); length = strlen(record);
	sprintf(buffer + length, ",RCODE=%d", rcode);

	retval = ofcom_msg_sys_time(sysdate, systime);
	if( retval < 0 ) return error_return(retval, "ofcom_msg_sys_time()");

	retval = ofcom_log_file_path2(OFCOM_LOG_TYPE_COMMAND, "dstool", sysdate, fpath);
	if( retval < 0 ) return error_return(retval, "ofcom_log_file_path2()");

	retval = ofcom_log_a_record2(fpath, systime, title, buffer, rcode);
	if( retval < 0 ) return error_return(retval, "ofcom_log_a_record2()");

	return 0;
}
