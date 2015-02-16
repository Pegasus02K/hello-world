/* Dataset Copy
 *
 * Description:
 *      Copy an Existing Dataset
 * 
 * Used service:
 *      OFRUISVRDSCOPY2
 *
 * To service:
 *      FB_TACF_TOKEN(string): TACF token
 *      FB_DSNAME(string): source dataset name
 *      FB_CATNAME(string): catalog name of the task (optional)
 *      FB_VOLUME(string): source volume serial (optional)
 *      FB_ARGS(string): destination parameters
 *
 * Format(FB_ARGS):
 *      dst_dsname;dst_volser
 *    - dst_dsname: destination dataset name (optional)
 *    - dst_volser: destination volume serial (optional)
 *    * NOTE: at least one destination parameter is compulsory
 *
 * From service:
 *      FB_RETMSG(string): error message
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#include <signal.h>
#include <usrinc/atmi.h>
#include <usrinc/fbuf.h>
#include <usrinc/tmaxapi.h>
#include <oframe_fdl.h>
#include "ofcom.h"
#include "ds.h"
#include "dscom.h"
#include "nvsm.h"

extern char *dscopy_version;
extern char *dscopy_build_info;

char dscopy_src_dsname[DS_DSNAME_LEN + 2] = {0,};
char dscopy_src_catname[DS_DSNAME_LEN + 2] = {0,};
char dscopy_src_volser[DS_VOLSER_LEN + 2] = {0,};

char dscopy_dst_dsname[DS_DSNAME_LEN + 2] = {0,};
char dscopy_dst_volser[DS_VOLSER_LEN + 2] = {0,};
char dscopy_dst_args[128] = {0,};

int error_return(int error_code, char *function_name);
int system_error(char *function_name);

int check_args(int argc, char *argv[]);
int print_usage();

int validate_param();

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
	if(argc >= 2 && ! strcmp(argv[1], "-V"))
	{
		printf("dscopy version %s %s\n", dscopy_version+26, dscopy_build_info+22);
		fflush(stdout); return 0;
	}
	else
	{
		printf("dscopy version %s %s\n", dscopy_version+26, dscopy_build_info+22);
		printf("Copy an Existing Dataset\n\n"); fflush(stdout);
	}

	/* check input arguments */
	retval = check_args(argc, argv);
	if( retval < 0 ) { print_usage(); return -1; }

	/* validate parameters */
	retval = validate_param();
	if( retval < 0 ) goto _DSCOPY_MAIN_ERR_RETURN_00;
	
	/* compose trace log record */
	sprintf(record, "SOURCE=%s,DEST=%s,CATALOG=%s,VOLSER=%s,TARGET=%s",
		dscopy_src_dsname, dscopy_dst_dsname, dscopy_src_catname, dscopy_src_volser, dscopy_dst_volser);

	/* print a log message */
	printf("DSCOPY %s\n", record); fflush(stdout);
	retval = 0;

	/* tool login process */
	retval = dscom_tool_login("DSCOPY");
	if( retval < 0 ) goto _DSCOPY_MAIN_ERR_RETURN_01;

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
	if ( snd_buf == NULL )
	{
		fprintf(stderr, "dscopy: ***Couldn't allocate send buffer->%s\n",tpstrerror(tperrno)); 
		goto _DSCOPY_MAIN_ERR_RETURN_02;
	}
	
	/* allocate receive buffer */
	rcv_buf = (FBUF *)tpalloc("FIELD", NULL, 0);
	if ( rcv_buf == NULL )
	{
		fprintf(stderr, "dscopy: ***Couldn't allocate send buffer->%s\n",tpstrerror(tperrno)); 
		tpfree((char *)snd_buf);
		goto _DSCOPY_MAIN_ERR_RETURN_02;
	}
	
	/* fbput FB_DSNAME */
	retval = fbput(snd_buf, FB_DSNAME, dscopy_src_dsname, 0);
	if ( retval == -1 )
	{
		fprintf(stderr, "dscopy: ***An error occurred while storing DSNAME in field buffer->%s\n", fbstrerror(fberror)); 
		goto _DSCOPY_MAIN_ERR_RETURN_03;
	}
	
	/* fbput FB_CATNAME */
	retval = fbput(snd_buf, FB_CATNAME, dscopy_src_catname, 0);
	if ( retval == -1 )
	{
		fprintf(stderr, "dscopy: ***An error occurred while storing CATNAME in field buffer->%s\n", fbstrerror(fberror)); 
		goto _DSCOPY_MAIN_ERR_RETURN_03;
	}
	
	/* fbput FB_VOLUME */
	retval = fbput(snd_buf, FB_VOLUME, dscopy_src_volser, 0);
	if ( retval == -1 )
	{
		fprintf(stderr, "dscopy: ***An error occurred while storing VOLUME in field buffer->%s\n", fbstrerror(fberror)); 
		goto _DSCOPY_MAIN_ERR_RETURN_03;
	}
	
	/* fbput FB_ARGS */
	sprintf(dscopy_dst_args, "%s;%s", dscopy_dst_dsname, dscopy_dst_volser);
	retval = fbput(snd_buf, FB_ARGS, dscopy_dst_args, 0);
	if ( retval == -1 )
	{
		fprintf(stderr, "dscopy: ***An error occurred while storing ARGS in field buffer->%s\n", fbstrerror(fberror)); 
		goto _DSCOPY_MAIN_ERR_RETURN_03;
	}
	
	/* tmax service call */
	retval = tpcall("OFRUISVRDSCOPY2", (char *)snd_buf, 0, (char **)&rcv_buf, &rcv_len, TPNOFLAGS);
	if (retval < 0) 
	{	// an error occurred
		fprintf(stderr, "dscopy: ***An error occurred in server OFRUISVRDSCOPY2->%s\n", tpstrerror(tperrno));
		retval = fbget(rcv_buf, FB_RETMSG, retmsg, 0);
		fprintf(stderr, "----------------Return message----------------\n%s\n----------------------------------------------\n", retmsg);
		goto _DSCOPY_MAIN_ERR_RETURN_03;
	}
	
	/* print a log message */
	printf("COMPLETED SUCCESSFULLY.\n"); fflush(stdout);
	retval = 0;

_DSCOPY_MAIN_ERR_RETURN_03:
	/* release buffers */
	tpfree((char *)rcv_buf);
	tpfree((char *)snd_buf);

_DSCOPY_MAIN_ERR_RETURN_02:
	/* tool logout process */
	dscom_tool_logout();

_DSCOPY_MAIN_ERR_RETURN_01:
	/* call a function to log a record */
	log_a_record("DSCOPY", record, retval);

_DSCOPY_MAIN_ERR_RETURN_00:
	/* process returns here */
	return retval;
}


int error_return(int error_code, char *function_name)
{
	fprintf(stderr, "dscopy: *** %s failed - errcode=%d\n", function_name, error_code);
	return error_code;
}


int system_error(char *function_name)
{
	fprintf(stderr, "dscopy: *** %s failed - error=%s\n", function_name, strerror(errno));
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
		if( ! strcmp(argv[i], "-c") ) {
			SKIP_ARGI_OR_RETURN_ERROR(i, argc);
			if( strlen(argv[i]) > DS_DSNAME_LEN ) return -1;
			strcpy(dscopy_src_catname, argv[i]);

		/* check if -v option is specified */
		} else if( ! strcmp(argv[i], "-v") ) {
			SKIP_ARGI_OR_RETURN_ERROR(i, argc);
			if( strlen(argv[i]) > DS_VOLSER_LEN ) return -1;
			strcpy(dscopy_src_volser,  argv[i]);

		/* check if -t option is specified */
		} else if( ! strcmp(argv[i], "-t") ) {
			SKIP_ARGI_OR_RETURN_ERROR(i, argc);
			if( strlen(argv[i]) > DS_VOLSER_LEN ) return -1;
			strcpy(dscopy_dst_volser,  argv[i]);

		/* otherwise <dsname> specified */
		} else {
			if( argv[i][0] == '-' ) return -1;
			if( strlen(argv[i]) > DS_DSNAME_LEN ) return -1;

			if( dscopy_dst_dsname[0] ) return -1;
			if( dscopy_src_dsname[0] ) strcpy(dscopy_dst_dsname, argv[i]);
			else strcpy(dscopy_src_dsname, argv[i]);
		}
	}

	/* check if source dataset name or dest dataset name is not specified */
	if( ! dscopy_src_dsname[0] || ! dscopy_dst_dsname[0] ) return -1;

	return 0;
}


int print_usage()
{
	printf("Usage: dscopy [options] <source> <dest>\n");
	printf("<source>    Specifies the source dataset name\n");
	printf("<dest>      Specifies the destination dataset name\n");
	printf("\n");

	printf("Options:\n");
	printf("  -c        Specifies the catalog name that contains source dataset entry\n");
	printf("  -v        Specifies the volume serial where the source dataset to be found\n");
	printf("  -t        Specifies the target volume serial to where the dataset to be copied\n");
	printf("\n");

	return 0;
}


int validate_param()
{
	/* check if source dataset name is specified */
	if( dscopy_src_dsname[0] != '\0' ) {
		/* check if source dataset name is invalid */
		if( dscom_check_dsname(dscopy_src_dsname) < 0 ) {
			fprintf(stderr, "dscopy: *** invalid source dataset name specified - source=%s\n", dscopy_src_dsname);
			return -1;
		}
	}

	/* check if destination dataset name is specified */
	if( dscopy_dst_dsname[0] != '\0' ) {
		/* check if destination dataset name is invalid */
		if( dscom_check_dsname(dscopy_dst_dsname) < 0 ) {
			fprintf(stderr, "dscopy: *** invalid destination dataset name specified - dest=%s\n", dscopy_dst_dsname);
			return -1;
		}
	}

	/* check if user catalog name is specified */
	if( dscopy_src_catname[0] != '\0' ) {
		/* check if user catalog name is invalid */
		if( dscom_check_dsname(dscopy_src_catname) < 0 ) {
			fprintf(stderr, "dscopy: *** invalid user catalog name specified - catalog=%s\n", dscopy_src_catname);
			return -1;
		}
	}

	/* check if source volume serial is specified */
	if( dscopy_src_volser[0] != '\0' ) {
		/* check if source volume serial is invalid */
		if( dscom_check_volser(dscopy_src_volser) < 0 ) {
			fprintf(stderr, "dscopy: *** invalid source volume serial specified - volser=%s\n", dscopy_src_volser);
			return -1;
		}
	}

	/* check if target volume serial is specified */
	if( dscopy_dst_volser[0] != '\0' ) {
		/* check if target volume serial is invalid */
		if( dscom_check_volser(dscopy_dst_volser) < 0 ) {
			fprintf(stderr, "dscopy: *** invalid target volume serial specified - target=%s\n", dscopy_dst_volser);
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
