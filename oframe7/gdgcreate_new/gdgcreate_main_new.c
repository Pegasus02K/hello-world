/* Generation Data Group Create
 *
 * Description:
 *      Create a new GDG
 *
 * Used service:
 *      OFRUISVRGDGCRE
 *
 * Parameters to service:
 *      FB_TACF_TOKEN(string): TACF token
 *      FB_DSNAME(string): GDG name
 *      FB_ARGS(string): GDG attributes
 *
 * Format(FB_ARGS):
 *      limit;expire;usercat;empty;scratch;
 *
 * Return values from service:
 *      FB_RETMSG(string): error message
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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

extern char *gdgcreate_version;
extern char *gdgcreate_build_info;

int32_t gdgcreate_limit = 255;
char gdgcreate_gdgname[DS_DSNAME_LEN + 2] = {0,};
char gdgcreate_catalog[DS_DSNAME_LEN + 2] = {0,};
char gdgcreate_args[1024] = {0,};
char gdgcreate_expdt[8+2] = {0,};
int gdgcreate_empty = 0;
int gdgcreate_scratch = 0;

int error_return(int error_code, char *function_name);
int system_error(char *function_name);
int check_args(int argc, char *argv[]);
int print_usage();
int validate_param();
int set_field_buffer(FBUF * fbuf);
int log_a_record(char *title, char *record, int rcode);

static void _signal_handler(int signo)
{
	switch(signo)
	{
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
		printf("gdgcreate version %s %s\n", gdgcreate_version+26, gdgcreate_build_info+22);
		fflush(stdout); return 0;
	} else {
		printf("gdgcreate version %s %s\n", gdgcreate_version+26, gdgcreate_build_info+22);
		printf("Create a New GDG (Generation Data Group)\n\n"); fflush(stdout);
	}

	/* check input arguments */
	retval = check_args(argc, argv);
	if( retval < 0 ) { print_usage(); return -1; }

	/* validate parameters */
	retval = validate_param();
	if( retval < 0 ) goto _GDGCREATE_MAIN_ERR_RETURN_00;
	sprintf(gdgcreate_args, "%d;%s;%s;%d;%d;", gdgcreate_limit, gdgcreate_expdt, gdgcreate_catalog, gdgcreate_empty, gdgcreate_scratch);
	
	/* compose trace log record */
	sprintf(record, "GDGNAME=%s,CATALOG=%s,LIMIT=%d",
		gdgcreate_gdgname, gdgcreate_catalog, gdgcreate_limit);

	/* print a log message */
	printf("GDGCREATE %s\n", record); fflush(stdout);
	retval = 0;

	/* tool login process */
	retval = dscom_tool_login("GDGCREATE");
	if( retval < 0 ) goto _GDGCREATE_MAIN_ERR_RETURN_01;

	/* catch signals */
	if(signal(SIGINT, _signal_handler) == SIG_ERR)
	{
		fprintf(stderr, "Cannot handle SIGINT!\n");
		exit(EXIT_FAILURE);
	}
	if(signal(SIGTERM, _signal_handler) == SIG_ERR)
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
		fprintf(stderr, "gdgcreate: ***Couldn't allocate send buffer->%s\n",tpstrerror(tperrno)); 
		goto _GDGCREATE_MAIN_ERR_RETURN_02;
	}
	
	/* allocate receive buffer */
	rcv_buf = (FBUF *)tpalloc("FIELD", NULL, 0);
	if(rcv_buf == NULL)
	{
		fprintf(stderr, "gdgcreate: ***Couldn't allocate send buffer->%s\n",tpstrerror(tperrno)); 
		tpfree((char *)snd_buf);
		goto _GDGCREATE_MAIN_ERR_RETURN_02;
	}
	
	/* set field buffer parameters */
	retval = set_field_buffer(snd_buf);
	if (retval < 0)
		goto _GDGCREATE_MAIN_ERR_RETURN_03;
	
	/* tmax service call */
	retval = tpcall("OFRUISVRGDGCRE", (char *)snd_buf, 0, (char **)&rcv_buf, &rcv_len, TPNOFLAGS);
	if (retval < 0) 
	{	// an error occurred
		fprintf(stderr, "gdgcreate: ***An error occurred in server OFRUISVRGDGCRE->%s\n", tpstrerror(tperrno));
		retval = fbget(rcv_buf, FB_RETMSG, retmsg, 0);		
		fprintf(stderr, "----------------Return message----------------\n%s\n----------------------------------------------\n", retmsg);
		goto _GDGCREATE_MAIN_ERR_RETURN_03;
	}

	/* print a log message */
	printf("COMPLETED SUCCESSFULLY.\n"); fflush(stdout);
	retval = 0;

_GDGCREATE_MAIN_ERR_RETURN_03:
	/* release buffers */
	tpfree((char *)rcv_buf);
	tpfree((char *)snd_buf);

_GDGCREATE_MAIN_ERR_RETURN_02:
	/* tool logout process */
	dscom_tool_logout();

_GDGCREATE_MAIN_ERR_RETURN_01:
	/* call a function to log a record */
	log_a_record("GDGCREATE", record, retval);

_GDGCREATE_MAIN_ERR_RETURN_00:
	/* process returns here */
	return retval;
}


int error_return(int error_code, char *function_name)
{
	fprintf(stderr, "gdgcreate: *** %s failed - errcode=%d\n", function_name, error_code);
	return error_code;
}


int system_error(char *function_name)
{
	fprintf(stderr, "gdgcreate: *** %s failed - error=%s\n", function_name, strerror(errno));
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
			strcpy(gdgcreate_catalog, argv[i]);

		/* check if -l option is specified */
		} else if( ! strcmp(argv[i], "-l") || ! strcmp(argv[i], "--limit") ) {
			SKIP_ARGI_OR_RETURN_ERROR(i, argc);
			gdgcreate_limit = atoi(argv[i]);

		/* check if -x option is specified */
		} else if( ! strcmp(argv[i], "-x") || ! strcmp(argv[i], "--expdt") ) {
			SKIP_ARGI_OR_RETURN_ERROR(i, argc);
			if( strlen(argv[i]) != 8 ) return -1;
			strcpy(gdgcreate_expdt, argv[i]);

		/* check if -E option is specified */
		} else if( ! strcmp(argv[i], "-E") || ! strcmp(argv[i], "--empty") ) {
			gdgcreate_empty = 1;

		/* check if -S option is specified */
		} else if( ! strcmp(argv[i], "-S") || ! strcmp(argv[i], "--scratch") ) {
			gdgcreate_scratch = 1;

		/* otherwise <gdgname> specified */
		} else {
			if( argv[i][0] == '-' ) return -1;
			if( strlen(argv[i]) > DS_DSNAME_LEN - 9 ) return -1;

			if( gdgcreate_gdgname[0] ) return -1;
			strcpy(gdgcreate_gdgname, argv[i]);
		}
	}

	/* check if the GDG name is not specified */
	if( ! gdgcreate_gdgname[0] ) return -1;

	return 0;
}


int print_usage()
{
	printf("Usage: gdgcreate [options] <gdgname>\n");
	printf("<gdgname>           Specifies the GDG name to be created\n");
	printf("\n");

	printf("Options:\n");
	printf("  -c, --catalog     Specifies the catalog name in which the GDG to be defined\n");
	printf("  -l, --limit       Specifies the maximum number of generation datasets allowed\n");
	printf("  -x, --expdt       Specifies the expiration date (in YYYYMMDD format)\n");
	printf("  -E, --empty       All the generation datasets are to be uncataloged when limit exceeded\n");
	printf("  -S, --scratch     Scratches the generation dataset when rolled off\n");
	printf("\n");
	#if 0
	printf("EMPTY and SCRATCH options are formally supported only for catalog information in this version.\n");
	printf("All GDG entries created are always considered to have NOEMPTY and SCRATCH attributes actually.\n");
	printf("\n");
	#endif
	return 0;
}


int validate_param()
{
	/* check if GDG name is specified */
	if( gdgcreate_gdgname[0] != '\0' ) {
		/* check if GDG name is invalid */
		if( dscom_check_dsname(gdgcreate_gdgname) < 0 ) {
			fprintf(stderr, "gdgcreate: *** invalid GDG name specified - gdgname=%s\n", gdgcreate_gdgname);
			return -1;
		}
	}

	/* check if catalog name is specified */
	if( gdgcreate_catalog[0] != '\0' ) {
		/* check if catalog name is invalid */
		if( dscom_check_dsname(gdgcreate_catalog) < 0 ) {
			fprintf(stderr, "gdgcreate: *** invalid catalog name specified - catalog=%s\n", gdgcreate_catalog);
			return -1;
		}
	}

	/* check if GDG limit is invalid */
	if( dscom_check_gdglimit(gdgcreate_limit) < 0 ) {
		fprintf(stderr, "gdgcreate: *** invalid GDG limit specified - gdglimit=%d\n", gdgcreate_limit);
		return -1;
	}

	/* check if expiration date is specified */
	if( gdgcreate_expdt[0] != '\0' ) {
		if( dscom_check_expdt_yyyymmdd(gdgcreate_expdt) < 0 ) {
			fprintf(stderr, "gdgcreate: *** invalid expiration date specified - expdt=%s\n", gdgcreate_expdt);
			return -1;
		}
	}

	return 0;
}


int set_field_buffer(FBUF * fbuf)
{
	int retval;
	
	/* fbput FB_DSNAME */
	retval = fbput(fbuf, FB_DSNAME, gdgcreate_gdgname, 0);
	if (retval == -1)
	{
		fprintf(stderr, "gdgcreate: ***An error occurred while storing DSNAME in field buffer->%s\n", fbstrerror(fberror)); 
		return -1;
	}

	/* fbput FB_ARGS */
	retval = fbput(fbuf, FB_ARGS, gdgcreate_args, 0);
	if (retval == -1)
	{
		fprintf(stderr, "gdgcreate: ***An error occurred while storing ARGS in field buffer->%s\n", fbstrerror(fberror)); 
		return -1;
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
