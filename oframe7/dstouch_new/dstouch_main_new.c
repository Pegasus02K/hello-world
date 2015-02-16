/* Dataset Touch
 *
 * Description:
 *      Update dataset status
 *
 * Used service:
 *      OFRUISVRDSTOUCH
 *
 * To service:
 *      FB_TACF_TOKEN(string): TACF token
 *      FB_DSNAME(string): dataset name
 *      FB_MEMNAME(string): member name
 *      FB_VOLUME(string): volume serial
 *      FB_CATNAME(string): catalog name
 *      FB_TIMESTAMP(string): creation date
 *
 * From service:
 *      FB_RETMSG(string): error message
 * NOTE:
 * use of -s(system) option should be reconsidered
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <usrinc/atmi.h>
#include <usrinc/fbuf.h>
#include <usrinc/tmaxapi.h>
#include <oframe_fdl.h>
#include "ofcom.h"
#include "dscom.h"
#include "ds.h"
#include "nvsm.h"

char dstouch_dsname[DS_DSNAME_LEN + 2] = {0,};

char dstouch_catalog[DS_DSNAME_LEN + 2] = {0,};
char dstouch_volser[DS_VOLSER_LEN + 2] = {0,};
char dstouch_member[NVSM_MEMBER_LEN + 2] = {0,};
char dstouch_credt[256] = {0,};

int dstouch_is_sysds = 0;

extern char *dstouch_version;
extern char *dstouch_build_info;

extern int dsalc_new_allocate_method;

int error_return(int error_code, char *function_name);

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
	if ((argc >= 2) && (!strcmp(argv[1], "-V")))
	{
		printf("dstouch version %s %s\n", dstouch_version+26, dstouch_build_info+22);
		fflush(stdout); return 0;
	}
	else
	{
		printf("dstouch version %s %s\n", dstouch_version+26, dstouch_build_info+22);
		printf("Update Dataset Status & Change Dataset Timestamp\n\n"); fflush(stdout);
	}

	/* check arguments */
	retval = check_args(argc, argv);
	if (retval < 0) { print_usage(); return -1; }

	/* validate parameters */
	retval = validate_param();
	if (retval < 0) goto _DSTOUCH_MAIN_ERR_RETURN_00;
	
	/* compose trace log record */
	sprintf(record, "DSNAME=%s,CATALOG=%s,VOLSER=%s,MEMBER=%s,DATE=%s",
		dstouch_dsname, dstouch_catalog, dstouch_volser, dstouch_member, dstouch_credt);

	/* print a log message */
	printf("DSTOUCH %s\n", record); fflush(stdout);
	retval = 0;

	/* tool login process */
	retval = dscom_tool_login("DSTOUCH");
	if (retval < 0) goto _DSTOUCH_MAIN_ERR_RETURN_01;

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
		fprintf(stderr, "dstouch: ***Couldn't allocate send buffer->%s\n",tpstrerror(tperrno)); 
		goto _DSTOUCH_MAIN_ERR_RETURN_02;
	}
	
	/* allocate receive buffer */
	rcv_buf = (FBUF *)tpalloc("FIELD", NULL, 0);
	if(rcv_buf == NULL)
	{
		fprintf(stderr, "dstouch: ***Couldn't allocate send buffer->%s\n",tpstrerror(tperrno)); 
		tpfree((char *)snd_buf);
		goto _DSTOUCH_MAIN_ERR_RETURN_02;
	}
	
	/* fbput FB_DSNAME */
	retval = fbput(snd_buf, FB_DSNAME, dstouch_dsname, 0);
	if (retval == -1)
	{
		fprintf(stderr, "dstouch: ***An error occurred while storing DSNAME in field buffer->%s\n", fbstrerror(fberror)); 
		goto _DSTOUCH_MAIN_ERR_RETURN_03;
	}
	
	/* fbput FB_MEMNAME */
	retval = fbput(snd_buf, FB_MEMNAME, dstouch_member, 0);
	if (retval == -1)
	{
		fprintf(stderr, "dstouch: ***An error occurred while storing MEMNAME in field buffer->%s\n", fbstrerror(fberror)); 
		goto _DSTOUCH_MAIN_ERR_RETURN_03;
	}
	
	/* fbput FB_VOLUME */
	retval = fbput(snd_buf, FB_VOLUME, dstouch_volser, 0);
	if (retval == -1)
	{
		fprintf(stderr, "dstouch: ***An error occurred while storing VOLUME in field buffer->%s\n", fbstrerror(fberror)); 
		goto _DSTOUCH_MAIN_ERR_RETURN_03;
	}
	
	/* fbput FB_CATNAME */
	retval = fbput(snd_buf, FB_CATNAME, dstouch_catalog, 0);
	if (retval == -1)
	{
		fprintf(stderr, "dstouch: ***An error occurred while storing CATNAME in field buffer->%s\n", fbstrerror(fberror)); 
		goto _DSTOUCH_MAIN_ERR_RETURN_03;
	}
	
	/* fbput FB_TIMESTAMP */
	retval = fbput(snd_buf, FB_TIMESTAMP, dstouch_credt, 0);
	if (retval == -1)
	{
		fprintf(stderr, "dstouch: ***An error occurred while storing TIMESTAMP in field buffer->%s\n", fbstrerror(fberror)); 
		goto _DSTOUCH_MAIN_ERR_RETURN_03;
	}
	
	/* tmax service call */
	retval = tpcall("OFRUISVRDSTOUCH", (char *)snd_buf, 0, (char **)&rcv_buf, &rcv_len, TPNOFLAGS);
	if (retval < 0) 
	{	// an error occurred
		fprintf(stderr, "dscreate: ***An error occurred in server OFRUISVRDSTOUCH->%s\n", tpstrerror(tperrno));
		retval = fbget(rcv_buf, FB_RETMSG, retmsg, 0);		
		fprintf(stderr, "----------------Return message----------------\n%s\n----------------------------------------------\n", retmsg);
		goto _DSTOUCH_MAIN_ERR_RETURN_03;
	}
	
	/* print log message */
	printf("COMPLETED SUCCESSFULLY.\n"); fflush(stdout);
	retval = 0;

_DSTOUCH_MAIN_ERR_RETURN_03:
	/* release buffers */
	tpfree((char *)rcv_buf);
	tpfree((char *)snd_buf);

_DSTOUCH_MAIN_ERR_RETURN_02:
	/* tool logout process */
	dscom_tool_logout();
	
_DSTOUCH_MAIN_ERR_RETURN_01:
	/* call a function to log a record */
	log_a_record("DSTOUCH", record, retval);

_DSTOUCH_MAIN_ERR_RETURN_00:
	/* process returns here */
	return retval;
}


int error_return(int error_code, char *function_name)
{
	fprintf(stderr, "dsmove: *** %s failed - errcode=%d\n", function_name, error_code);
	return error_code;
}


int check_args(int argc, char *argv[])
{
	int i;

	typedef enum argument_e {
		DATASET_NAME,
		MEMBER_NAME,
		END_ARGUMENT
	} argument_t;

	argument_t mode = DATASET_NAME;

	/* check minimum argument count */
	if (argc < 2) return -1;

	/* argument parsing */
	for (i  = 1; i < argc; i++) {
		/* options */
		if( argv[i][0] == '-' ) {
			if( ! strcmp(argv[i], "-s") ) {
				if( mode != DATASET_NAME ) return -1;
				dstouch_is_sysds = 1;
			} else if( ! strcmp(argv[i], "-v") ) {
				if( mode == MEMBER_NAME ) mode = END_ARGUMENT;
				if( mode != END_ARGUMENT ) return -1;
				if( i < argc-1 ) i++; else return -1;
				strcpy(dstouch_volser, argv[i]);
			} else if( ! strcmp(argv[i], "-c") ) {
				if( mode == MEMBER_NAME ) mode = END_ARGUMENT;
				if( mode != END_ARGUMENT ) return -1;
				if( i < argc-1 ) i++; else return -1;
				strcpy(dstouch_catalog, argv[i]);
			} else if( ! strcmp(argv[i], "-d") ) {
				if( mode == MEMBER_NAME ) mode = END_ARGUMENT;
				if( mode != END_ARGUMENT ) return -1;
				if( i < argc-1 ) i++; else return -1;
				strcpy(dstouch_credt, argv[i]);
			} else return -1;
		/* otherwise */
		} else {
			if( mode == DATASET_NAME ) {
				strcpy( dstouch_dsname, argv[i] );
				mode = MEMBER_NAME;
			} else if( mode == MEMBER_NAME ) {
				strcpy( dstouch_member, argv[i] );
				mode = END_ARGUMENT;
			} else return -1;
		}
	}

	return 0;
}


int print_usage()
{
	printf("Usage: dstouch [-s] <dsname> [<member>] [-v <volser>] [-c <catalog>] [-d <date>]\n"
           "<dsname>    Specifies the dataset name to be touched\n"
           "<member>    Specifies the member name of a PDS dataset\n\n");

	printf("Options:\n"
           "  -s        Specifies that the dataset to be touched is a system dataset\n"
           "  -v        Specifies volume serial (compulsory if dataset is not cataloged)\n"
           "  -c        Specifies catalog name (if dataset is cataloged in user catalog)\n"
           "  -d        Specifies creation date of the dataset to be set\n\n");

	return 0;
}



int validate_param()
{
	/* check if dataset name is specified */
	if( dstouch_dsname[0] != '\0' ) {
		/* check if dataset name is invalid */
		if( dscom_check_dsname(dstouch_dsname) < 0 ) {
			fprintf(stderr, "dstouch: *** invalid dataset name specified - dsname=%s\n", dstouch_dsname);
			return -1;
		}
	}

	/* check if member name is specified */
	if( dstouch_member[0] != '\0' ) {
		/* check if member name is invalid */
		if( dscom_check_dsname2(dstouch_dsname, dstouch_member) < 0 ) {
			fprintf(stderr, "dstouch: *** invalid dataset name or member name specified - dsname=%s,member=%s\n", dstouch_dsname, dstouch_member);
			return -1;
		}
	}

	/* check if user catalog is specified */
	if( dstouch_catalog[0] != '\0' ) {
		/* check if user catalog is invalid */
		if( dscom_check_dsname(dstouch_catalog) < 0 ) {
			fprintf(stderr, "dstouch: *** invalid user catalog specified - catalog=%s\n", dstouch_catalog);
			return -1;
		}
	}

	/* check if volume serial is specified */
	if( dstouch_volser[0] != '\0' ) {
		/* check if volume serial is invalid */
		if( dscom_check_volser(dstouch_volser) < 0 ) {
			fprintf(stderr, "dstouch: *** invalid volume serial specified - volser=%s\n", dstouch_volser);
			return -1;
		}
	}

	/* check if creation date is specified */
	if( dstouch_credt[0] != '\0' ) {
		/* check if creation date is invalid */
		if( dscom_check_credt(dstouch_credt) < 0) {
			fprintf(stderr, "dstouch: *** invalid creation date specified - date=%s\n", dstouch_credt);
			return -1;
		}
	} else {
		/* set default date(sysdate) */
		OFCOM_DATE_TO_STRING( dstouch_credt, ofcom_sys_date() );
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
