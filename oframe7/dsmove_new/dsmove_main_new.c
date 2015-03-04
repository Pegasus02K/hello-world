/* Dataset Move
 *
 * Description:
 *      Move or rename an existing dataset
 *
 * Used service:
 *      OFRUISVRDSMOVE2
 * 
 * Parameters to service:
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
 * Return values from service:
 *      FB_RETMSG(string): error message
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
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

extern char *dsmove_version;
extern char *dsmove_build_info;

char dsmove_src_dsname[DS_DSNAME_LEN + 2] = {0,};
char dsmove_src_catname[DS_DSNAME_LEN + 2] = {0,};
char dsmove_src_volser[DS_VOLSER_LEN + 2] = {0,};
char dsmove_dst_dsname[DS_DSNAME_LEN + 2] = {0,};
char dsmove_dst_volser[DS_VOLSER_LEN + 2] = {0,};
char dsmove_dst_args[128] = {0,};

int error_return(int error_code, char *function_name);
int system_error(char *function_name);
int check_args(int argc, char *argv[]);
int print_usage();
int validate_param();
int set_field_buffer(FBUF * fbuf);
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
		printf("dsmove version %s %s\n", dsmove_version+26, dsmove_build_info+22);
		fflush(stdout); return 0;
	}
	else
	{
		printf("dsmove version %s %s\n", dsmove_version+26, dsmove_build_info+22);
		printf("Move or Rename a Dataset\n\n"); fflush(stdout);
	}

	/* check input arguments */
	retval = check_args(argc, argv);
	if( retval < 0 ) { print_usage(); return -1; }

	/* validate parameters */
	retval = validate_param();
	if( retval < 0 ) goto _DSMOVE_MAIN_ERR_RETURN_00;
	
	sprintf(dsmove_dst_args, "%s;%s", dsmove_dst_dsname, dsmove_dst_volser);
	
	/* compose trace log record */
	sprintf(record, "SOURCE=%s,DEST=%s,CATALOG=%s,VOLSER=%s,TARGET=%s",
		dsmove_src_dsname, dsmove_dst_dsname, dsmove_src_catname, dsmove_src_volser, dsmove_dst_volser);

	/* print a log message */
	printf("DSMOVE %s\n", record); fflush(stdout);

	/* tool login process */
	retval = dscom_tool_login("DSMOVE");
	if( retval < 0 ) goto _DSMOVE_MAIN_ERR_RETURN_01;

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
		fprintf(stderr, "dsmove: ***Couldn't allocate send buffer->%s\n",tpstrerror(tperrno)); 
		goto _DSMOVE_MAIN_ERR_RETURN_02;
	}
	
	/* allocate receive buffer */
	rcv_buf = (FBUF *)tpalloc("FIELD", NULL, 0);
	if ( rcv_buf == NULL )
	{
		fprintf(stderr, "dsmove: ***Couldn't allocate send buffer->%s\n",tpstrerror(tperrno)); 
		tpfree((char *)snd_buf);
		goto _DSMOVE_MAIN_ERR_RETURN_02;
	}
	
	/* set field buffer parameters */
	retval = set_field_buffer(snd_buf);
	if (retval < 0)
		goto _DSMOVE_MAIN_ERR_RETURN_03;
	
	/* tmax service call */
	retval = tpcall("OFRUISVRDSMOVE2", (char *)snd_buf, 0, (char **)&rcv_buf, &rcv_len, TPNOFLAGS);
	if (retval < 0) 
	{	// an error occurred
		fprintf(stderr, "dscopy: ***An error occurred in server OFRUISVRDSCOPY2->%s\n", tpstrerror(tperrno));
		retval = fbget(rcv_buf, FB_RETMSG, retmsg, 0);
		fprintf(stderr, "----------------Return message----------------\n%s\n----------------------------------------------\n", retmsg);
		goto _DSMOVE_MAIN_ERR_RETURN_03;
	}
	
	/* print a log message */
	printf("COMPLETED SUCCESSFULLY.\n"); fflush(stdout);
	retval = 0;

_DSMOVE_MAIN_ERR_RETURN_03:
	/* release buffers */
	tpfree((char *)rcv_buf);
	tpfree((char *)snd_buf);

_DSMOVE_MAIN_ERR_RETURN_02:
	/* tool logout process */
	dscom_tool_logout();

_DSMOVE_MAIN_ERR_RETURN_01:
	/* call a function to log a record */
	log_a_record("DSMOVE", record, retval);

_DSMOVE_MAIN_ERR_RETURN_00:
	/* process returns here */
	if (retval)
	{
		printf("ABNORMALLY FINISHED (Return value: %d)\n",retval);
		fflush(stdout);
	}
	return retval;
}


int error_return(int error_code, char *function_name)
{
	fprintf(stderr, "dsmove: *** %s failed - errcode=%d\n", function_name, error_code);
	return error_code;
}


int system_error(char *function_name)
{
	fprintf(stderr, "dsmove: *** %s failed - error=%s\n", function_name, strerror(errno));
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
			strcpy(dsmove_src_catname, argv[i]);

		/* check if -v option is specified */
		} else if( ! strcmp(argv[i], "-v") ) {
			SKIP_ARGI_OR_RETURN_ERROR(i, argc);
			if( strlen(argv[i]) > DS_VOLSER_LEN ) return -1;
			strcpy(dsmove_src_volser,  argv[i]);

		/* check if -t option is specified */
		} else if( ! strcmp(argv[i], "-t") ) {
			SKIP_ARGI_OR_RETURN_ERROR(i, argc);
			if( strlen(argv[i]) > DS_VOLSER_LEN ) return -1;
			strcpy(dsmove_dst_volser,  argv[i]);

		/* otherwise <dsname> specified */
		} else {
			if( argv[i][0] == '-' ) return -1;
			if( strlen(argv[i]) > DS_DSNAME_LEN ) return -1;

			if( dsmove_dst_dsname[0] ) return -1;
			if( dsmove_src_dsname[0] ) strcpy(dsmove_dst_dsname, argv[i]);
			else strcpy(dsmove_src_dsname, argv[i]);
		}
	}

	/* check if source dataset name or dest dataset name is not specified */
	if( ! dsmove_src_dsname[0] || ! dsmove_dst_dsname[0] ) return -1;

	return 0;
}


int print_usage()
{
	printf("Usage: dsmove [options] <source> <dest>\n");
	printf("<source>    Specifies the source dataset name\n");
	printf("<dest>      Specifies the destination dataset name\n");
	printf("\n");

	printf("Options:\n");
	printf("  -c        Specifies the catalog name that contains source dataset entry\n");
	printf("  -v        Specifies the volume serial where the source dataset to be found\n");
	printf("  -t        Specifies the target volume serial to where the dataset to be moved\n");
	printf("\n");

	return 0;
}


int validate_param()
{
	/* check if source dataset name is specified */
	if( dsmove_src_dsname[0] != '\0' ) {
		/* check if source dataset name is invalid */
		if( dscom_check_dsname(dsmove_src_dsname) < 0 ) {
			fprintf(stderr, "dsmove: *** invalid source dataset name specified - source=%s\n", dsmove_src_dsname);
			return -1;
		}
	}

	/* check if destination dataset name is specified */
	if( dsmove_dst_dsname[0] != '\0' ) {
		/* check if destination dataset name is invalid */
		if( dscom_check_dsname(dsmove_dst_dsname) < 0 ) {
			fprintf(stderr, "dsmove: *** invalid destination dataset name specified - dest=%s\n", dsmove_dst_dsname);
			return -1;
		}
	}

	/* check if user catalog name is specified */
	if( dsmove_src_catname[0] != '\0' ) {
		/* check if user catalog name is invalid */
		if( dscom_check_dsname(dsmove_src_catname) < 0 ) {
			fprintf(stderr, "dsmove: *** invalid user catalog name specified - catalog=%s\n", dsmove_src_catname);
			return -1;
		}
	}

	/* check if source volume serial is specified */
	if( dsmove_src_volser[0] != '\0' ) {
		/* check if source volume serial is invalid */
		if( dscom_check_volser(dsmove_src_volser) < 0 ) {
			fprintf(stderr, "dsmove: *** invalid source volume serial specified - volser=%s\n", dsmove_src_volser);
			return -1;
		}
	}

	/* check if target volume serial is specified */
	if( dsmove_dst_volser[0] != '\0' ) {
		/* check if target volume serial is invalid */
		if( dscom_check_volser(dsmove_dst_volser) < 0 ) {
			fprintf(stderr, "dsmove: *** invalid target volume serial specified - target=%s\n", dsmove_dst_volser);
			return -1;
		}
	}

	/* check if wild card is used */
	if( strchr(dsmove_src_dsname, '*') || strchr(dsmove_src_dsname, '%') ) {
		fprintf(stderr, "dsmove: *** wild card character is not allowed in dsname. source=%s\n", dsmove_src_dsname);
		return -1;

	/* else if member name specified */
	} else if( strchr(dsmove_src_dsname, '(') || strchr(dsmove_src_dsname, ')') ) {
		fprintf(stderr, "dsmove: *** member of a PDS dataset is not supported. source=%s\n", dsmove_src_dsname);
		return -1;
	}

	/* check if wild card is used */
	if( strchr(dsmove_dst_dsname, '*') || strchr(dsmove_dst_dsname, '%') ) {
		fprintf(stderr, "dsmove: *** wild card character is not allowed in dsname. dest=%s\n", dsmove_dst_dsname);
		return -1;

	/* else if member name is specified */
	} else if( strchr(dsmove_dst_dsname, '(') || strchr(dsmove_dst_dsname, ')') ) {
		fprintf(stderr, "dsmove: *** member of a PDS dataset is not supported. dest=%s\n", dsmove_dst_dsname);
		return -1;
	}

	return 0;
}


int set_field_buffer(FBUF * fbuf)
{
	int retval;
	
	/* fbput FB_DSNAME */
	retval = fbput(fbuf, FB_DSNAME, dsmove_src_dsname, 0);
	if ( retval == -1 )
	{
		fprintf(stderr, "dsmove: ***An error occurred while storing DSNAME in field buffer->%s\n", fbstrerror(fberror));
		return -1;
	}
	
	/* fbput FB_CATNAME */
	retval = fbput(fbuf, FB_CATNAME, dsmove_src_catname, 0);
	if ( retval == -1 )
	{
		fprintf(stderr, "dsmove: ***An error occurred while storing CATNAME in field buffer->%s\n", fbstrerror(fberror)); 
		return -1;
	}
	
	/* fbput FB_VOLUME */
	retval = fbput(fbuf, FB_VOLUME, dsmove_src_volser, 0);
	if ( retval == -1 )
	{
		fprintf(stderr, "dsmove: ***An error occurred while storing VOLUME in field buffer->%s\n", fbstrerror(fberror)); 
		return -1;
	}
	
	/* fbput FB_ARGS */
	retval = fbput(fbuf, FB_ARGS, dsmove_dst_args, 0);
	if ( retval == -1 )
	{
		fprintf(stderr, "dsmove: ***An error occurred while storing ARGS in field buffer->%s\n", fbstrerror(fberror)); 
		return -1;
	}
	
	return 0;
}


int log_a_record(char *title, char *record, int rcode)
{
	int retval, length; char buffer[4096], sysdate[16], systime[16], fpath[DS_PATHNAME_LEN+1];

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
