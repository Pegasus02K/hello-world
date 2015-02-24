/* Dataset Save
 *
 * Description:
 *      Dataset save program for external editor. 
 *
 * Used service:
 *      OFRUISVRDSSAVE
 *
 * Parameters to service:
 *      FB_TACF_TOKEN(string): TACF token
 *      FB_DSNAME(string): dataset name
 *      FB_MEMNAME(string): member name
 *      FB_CATNAME(string): catalog name
 *      FB_FILEPATH(string): dataset filepath
 *      FB_TYPE(char): save options (multiple, optional)
 *      FB_ARGS(string): delimiter 
 *
 * Format(FB_TYPE):
 *      L: use the delimiter to separate each record
 *      B: fills in spaces if a record is shorter 
 *      R: removes the source file after saving
 *
 * Return from service:
 *      FB_RETMSG(string): error message
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
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

extern char *dssave_version;
extern char *dssave_build_info;

char dssave_dsname[DS_DSNAME_LEN + 2] = {0,};
char dssave_member[NVSM_MEMBER_LEN + 2] = {0,};
char dssave_catalog[DS_DSNAME_LEN + 2] = {0,};
char dssave_srcpath[256] = {0,};
char dssave_deli_form[256] = {0,};

int _fill_spaces = 0;
int _remove_source = 0;
int _test_mode = 0;

int error_return(int error_code, char *function_name);

int check_args(int argc, char *argv[]);
int print_usage();
int validate_param();

int adjust_param();

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
	int retval;
	char record[1024];
	/* TPCALL variables */
	FBUF *snd_buf = NULL;
	FBUF *rcv_buf = NULL;
	long rcv_len;
	char retmsg[1024];
	
	/* print version and one line summary */
	if ((argc >= 2) && (!strcmp(argv[1],"-V")))
	{
		printf("dssave version %s %s\n", dssave_version+26, dssave_build_info+22);
		fflush(stdout); return 0;
	}
	else
	{
		printf("dssave version %s %s\n", dssave_version+26, dssave_build_info+22);
		printf("Dataset Save Program for External Editor\n\n"); fflush(stdout);
	}

	/* check arguments */
	retval = check_args(argc, argv);
	if (retval < 0) { print_usage(); return -1; }

	/* validate parameters */
	retval = validate_param();
	if (retval < 0) goto _DSSAVE_MAIN_ERR_RETURN_00;

	/* adjust parameters */
	retval = adjust_param();
	if (retval < 0) goto _DSSAVE_MAIN_ERR_RETURN_00;

	sprintf(record, "Source File        : [%s]\nDestination Dataset: [%s]\nDestination Member : [%s]\nUser Catalog       : [%s]\nDelimiter          : [%s]\n\n", dssave_srcpath, dssave_dsname, dssave_member, dssave_catalog, dssave_deli_form);
	
	/* print a log message */
	printf("DSSAVE\n%s", record); fflush(stdout);

	/* tool login process */
	retval = dscom_tool_login("DSSAVE");
	if (retval < 0) goto _DSSAVE_MAIN_ERR_RETURN_00;

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
		fprintf(stderr, "dssave: ***Couldn't allocate send buffer->%s\n",tpstrerror(tperrno)); 
		goto _DSSAVE_MAIN_ERR_RETURN_02;
	}
	
	/* allocate receive buffer */
	rcv_buf = (FBUF *)tpalloc("FIELD", NULL, 0);
	if(rcv_buf == NULL)
	{
		fprintf(stderr, "dssave: ***Couldn't allocate send buffer->%s\n",tpstrerror(tperrno)); 
		tpfree((char *)snd_buf);
		goto _DSSAVE_MAIN_ERR_RETURN_02;
	}
	
	/* fbput FB_DSNAME */
	retval = fbput(snd_buf, FB_DSNAME, dssave_dsname, 0);
	if (retval == -1)
	{
		fprintf(stderr, "dssave: ***An error occurred while storing DSNAME in field buffer->%s\n", fbstrerror(fberror)); 
		goto _DSSAVE_MAIN_ERR_RETURN_03;
	}
	
	/* fbput FB_MEMNAME */
	retval = fbput(snd_buf, FB_MEMNAME, dssave_member, 0);
	if (retval == -1)
	{
		fprintf(stderr, "dssave: ***An error occurred while storing MEMNAME in field buffer->%s\n", fbstrerror(fberror)); 
		goto _DSSAVE_MAIN_ERR_RETURN_03;
	}
	
	/* fbput FB_CATNAME */
	retval = fbput(snd_buf, FB_CATNAME, dssave_catalog, 0);
	if (retval == -1)
	{
		fprintf(stderr, "dssave: ***An error occurred while storing CATNAME in field buffer->%s\n", fbstrerror(fberror)); 
		goto _DSSAVE_MAIN_ERR_RETURN_03;
	}
	
	/* fbput FB_FILEPATH */
	retval = fbput(snd_buf, FB_FILEPATH, dssave_srcpath, 0);
	if (retval == -1)
	{
		fprintf(stderr, "dssave: ***An error occurred while storing FILEPATH in field buffer->%s\n", fbstrerror(fberror)); 
		goto _DSSAVE_MAIN_ERR_RETURN_03;
	}
	
	/* fbput FB_FILEPATH */
	retval = fbput(snd_buf, FB_FILEPATH, dssave_srcpath, 0);
	if (retval == -1)
	{
		fprintf(stderr, "dssave: ***An error occurred while storing FILEPATH in field buffer->%s\n", fbstrerror(fberror)); 
		goto _DSSAVE_MAIN_ERR_RETURN_03;
	}
	
	/* fbput FB_ARGS */
	retval = fbput(snd_buf, FB_ARGS, dssave_deliform, 0);
	if (retval == -1)
	{
		fprintf(stderr, "dssave: ***An error occurred while storing FILEPATH in field buffer->%s\n", fbstrerror(fberror)); 
		goto _DSSAVE_MAIN_ERR_RETURN_03;
	}
	
	/* fbput FB_TYPE */
	if (_fill_spaces)
	{
		retval = fbput(snd_buf, FB_TYPE, "B", 0);
		if (retval == -1)
		{
			fprintf(stderr, "dssave: ***An error occurred while storing TYPE in field buffer->%s\n", fbstrerror(fberror)); 
			goto _DSSAVE_MAIN_ERR_RETURN_03;
		}
	}
	if (_remove_source)
	{
		retval = fbput(snd_buf, FB_TYPE, "R", 0);
		if (retval == -1)
		{
			fprintf(stderr, "dssave: ***An error occurred while storing TYPE in field buffer->%s\n", fbstrerror(fberror)); 
			goto _DSSAVE_MAIN_ERR_RETURN_03;
		}
	}
	if (_test_mode)
	{
		retval = fbput(snd_buf, FB_TYPE, "T", 0);
		if (retval == -1)
		{
			fprintf(stderr, "dssave: ***An error occurred while storing TYPE in field buffer->%s\n", fbstrerror(fberror)); 
			goto _DSSAVE_MAIN_ERR_RETURN_03;
		}
	}
	
	/* tmax service call */
	retval = tpcall("OFRUISVRDSSAVE", (char *)snd_buf, 0, (char **)&rcv_buf, &rcv_len, TPNOFLAGS);
	if (retval < 0) 
	{	// an error occurred
		fprintf(stderr, "dssave: ***An error occurred in server OFRUISVRDSSAVE->%s\n", tpstrerror(tperrno));
		retval = fbget(rcv_buf, FB_RETMSG, retmsg, 0);		
		fprintf(stderr, "----------------Return message----------------\n%s\n----------------------------------------------\n", retmsg);
		goto _DSSAVE_MAIN_ERR_RETURN_03;
	}
	
	/* print message */
	printf("Dataset is saved successfully\n");
	retval = 0;

	/* check to remove source */
	if (_remove_source && ! _test_mode) {
		printf("Removing source file - %s\n", dssave_srcpath);
		unlink(dssave_srcpath);
	}
	
_DSSAVE_MAIN_ERR_RETURN_03:
	/* release buffers */
	tpfree((char *)rcv_buf);
	tpfree((char *)snd_buf);
	
_DSSAVE_MAIN_ERR_RETURN_02:
	/* tool logout process */
	dscom_tool_logout();
	
_DSSAVE_MAIN_ERR_RETURN_01:
	/* call a function to log a record */
	log_a_record("DSSAVE", record, retval);
	
_DSSAVE_MAIN_ERR_RETURN_00:
	/* process returns here */
	return retval;
}


int check_args(int argc, char *argv[])
{
	int i;

	/* check minimum argument count */
	if (argc < 2) return -1;
	strcpy(dssave_dsname,argv[1]);

	i = 2;
	while (argv[i]) {
		if (!strcmp(argv[i],"-c") || !strcmp(argv[i],"--catalog")) {
			if (argv[i+1]) {
				strcpy(dssave_catalog,argv[i+1]);
			} else {
				return -1;
			}
			i++;
		} else if (!strcmp(argv[i],"-m") || !strcmp(argv[i],"--member")) {
			if (argv[i+1]) {
				strcpy(dssave_member,argv[i+1]);
			} else {
				return -1;
			}
			i++;
		} else if (!strcmp(argv[i],"-s") || !strcmp(argv[i],"--source")) {
			if (argv[i+1]) {
				strcpy(dssave_srcpath,argv[i+1]);
			} else {
				return -1;
			}
			i++;
		} else if (!strcmp(argv[i],"-d") || !strcmp(argv[i],"--delimiter")) {
			if (argv[i+1]) {
				if (!strcmp(argv[i+1],"NEWLINE")) strcpy(dssave_deli_form, "\\n");
				else strcpy(dssave_deli_form, argv[i+1]);
			} else {
				return -1;
			}
			i++;
		} else if (!strcmp(argv[i],"-B") || !strcmp(argv[i],"--blank")) {
			_fill_spaces = 1;
		} else if (!strcmp(argv[i],"-R") || !strcmp(argv[i],"--remove")) {
			_remove_source = 1;
		} else if (!strcmp(argv[i],"-T") || !strcmp(argv[i],"--test")) {
			_test_mode = 1;
		}
		i++;
	}

	return 0;
}


int print_usage()
{
	printf("Usage: dssave <dsname> [options]\n");
	printf("<dsname>            Specifies the dataset name to be saved\n");
	printf("\n");

	printf("Options:\n");
	printf("  -c, --catalog     Specifies the catalog name that contains the dataset entry\n");
	printf("  -m, --member      Specifies the member name of the PDS dataset\n");
	printf("  -s, --source      Specifies the source filepath\n");
	printf("  -d, --delimiter   Specifies the delimiter to separate each record\n");
	printf("  -B, --blank       Fills in spaces if a record is shorter than the record length\n");
	printf("  -R, --remove      Removes the source file after saving the dataset\n");
	printf("  -T, --test        Activates the test mode run\n");
	printf("\n");

	return 0;
}
UIS0104E, %s: system(%s) failed. err=%s,rc=%d
UIS0031E, %s: file read error. fd=%d,err=%s

int adjust_param()
{
	int  retval;
	char s_temp[1024];

	/* use temp directory if filepath is not specified */
	if (dssave_srcpath[0] == 0x00) {
		retval = ofcom_conf_get_value("dstool.conf", "DSLOAD", "LOAD_DIR", s_temp, sizeof(s_temp));
		if (retval < 0) {
			fprintf(stderr, "dssave: *** Get configuration failed. dstool.conf [DSLOAD] LOAD_DIR\n");
			return -1;
		}

		strcpy(dssave_srcpath, s_temp);
		strcat(dssave_srcpath, "/");
		strcat(dssave_srcpath, dssave_dsname);

		if (dssave_member[0] != 0x00) {
			strcat(dssave_srcpath, ".");
			strcat(dssave_srcpath, dssave_member);
		}
	}

	/* use config setting if delimiter is not specified */
	if (dssave_deli_form[0] == 0x00) {
		retval = ofcom_conf_get_value("dstool.conf", "DSLOAD", "DELIMITER", s_temp, sizeof(s_temp));
		if (retval < 0) {
			strcpy(dssave_deli_form, "");
		} else {
			if (!strcmp(s_temp, "NEWLINE")) strcpy(dssave_deli_form, "\\n");
			else strcpy(dssave_deli_form, s_temp);
		}
	}

	return 0;
}


int validate_param()
{
	/* check if dataset name is specified */
	if( dssave_dsname[0] != '\0' ) {
		/* check if dataset name is invalid */
		if( dscom_check_dsname(dssave_dsname) < 0 ) {
			fprintf(stderr, "dssave: *** invalid dataset name specified - dsname=%s\n", dssave_dsname);
			return -1;
		}
	}

	/* check if member name is specified */
	if( dssave_member[0] != '\0' ) {
		/* check if member name is invalid */
		if( dscom_check_dsname2(dssave_dsname, dssave_member) < 0 ) {
			fprintf(stderr, "dssave: *** invalid dataset name or member name specified - dsname=%s,member=%s\n", dssave_dsname, dssave_member);
			return -1;
		}
	}

	/* check if catalog name is specified */
	if( dssave_catalog[0] != '\0' ) {
		/* check if catalog name is invalid */
		if( dscom_check_dsname(dssave_catalog) < 0 ) {
			fprintf(stderr, "dssave: *** invalid catalog name specified - catalog=%s\n", dssave_catalog);
			return -1;
		}
	}

	return 0;
}


int error_return(int error_code, char *function_name)
{
	fprintf(stderr, "dsmove: *** %s failed - errcode=%d\n", function_name, error_code);
	return error_code;
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
