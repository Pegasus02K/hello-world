/* Dataset Load
 *
 * Description:
 *      Dataset load program for external editor
 *
 * Used service:
 *        OFRUISVRDSLOAD
 *
 * Parameters to service:
 *        FB_TACF_TOKEN(string): TACF token
 *        FB_DSNAME(string): dataset name
 *        FB_MEMNAME(string): member name
 *        FB_CATNAME(string): catalog name
 *        FB_FILEPATH(string): dataset filepath
 *        FB_TYPE(char): load options (multiple, optional)
 *
 *        FB_VOLUME(string): volume serial (optional)
 *        FB_ARGS(string): delimeter from tool 
 *
 * Format(FB_TYPE):
 *        L: use the delimeter to separate each record
 *        F: ignore existance of the destination file
 *
 * Return from service:
 *        FB_RETMSG(string): error message
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
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

char dsload_dsname[DS_DSNAME_LEN + 2] = {0,};
char dsload_member[NVSM_MEMBER_LEN + 2] = {0,};
char dsload_catalog[DS_DSNAME_LEN + 2] = {0,};
char dsload_dstpath[256] = {0,};
char dsload_volser[DS_VOLSER_LEN+2] = {0,};
char dsload_deli_form[256] = {0,};
char dsload_delimeter[256] = {0,};

int _file_check = 1;

extern char *dsload_version;
extern char *dsload_build_info;


int check_args(int argc, char *argv[]);
int print_usage();
int validate_param();

int adjust_param();
int convert_delim();

int check_dataset_size(char *dsname, dsio_dcb_t **dcbs);
int dsload_dataset_inner(int fdin, int fdout, int maxlen, char *delim);

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
//	char record[1024];
	/* TPCALL variables */
	FBUF *snd_buf = NULL;
	FBUF *rcv_buf = NULL;
	long rcv_len;
	char retmsg[1024];

	/* print version and one line summary */
	if ((argc >= 2) && (!strcmp(argv[1],"-V")))
	{
		printf("dsload version %s %s\n", dsload_version+26, dsload_build_info+22);
		fflush(stdout); return 0;
	}
	else
	{
		printf("dsload version %s %s\n", dsload_version+26, dsload_build_info+22);
		printf("Dataset Load Program for External Editor\n\n"); fflush(stdout);
	}
	
	/* check arguments */
	retval = check_args(argc, argv);
	if (retval < 0) { print_usage(); return -1; }

	/* validate parameters */
	retval = validate_param();
	if (retval < 0) goto _DSLOAD_MAIN_ERR_RETURN_00;

	/* adjust parameters */
	retval = adjust_param();
	if (retval < 0) goto _DSLOAD_MAIN_ERR_RETURN_00;

	/* print load parameters */
	printf("Source Dataset         : [%s]\n", dsload_dsname);
	printf("Source Member      : [%s]\n", dsload_member);
	printf("Volser             : [%s]\n", dsload_volser);
	printf("User Catalog       : [%s]\n", dsload_catalog);
	printf("Destination File   : [%s]\n", dsload_dstpath);
	printf("Delimiter          : [%s]\n", dsload_deli_form);
	printf("\n");

	/* convert delimeter */
	retval = convert_delim();
	if (retval < 0) goto _DSLOAD_MAIN_ERR_RETURN_00;

	/* tool login process */
	retval = dscom_tool_login("DSLOAD");
	if (retval < 0) goto _DSLOAD_MAIN_ERR_RETURN_01;

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
		fprintf(stderr, "dsload: ***Couldn't allocate send buffer->%s\n",tpstrerror(tperrno)); 
		goto _DSLOAD_MAIN_ERR_RETURN_02;
	}
	
	/* allocate receive buffer */
	rcv_buf = (FBUF *)tpalloc("FIELD", NULL, 0);
	if(rcv_buf == NULL)
	{
		fprintf(stderr, "dsload: ***Couldn't allocate send buffer->%s\n",tpstrerror(tperrno)); 
		tpfree((char *)snd_buf);
		goto _DSLOAD_MAIN_ERR_RETURN_02;
	}
	
	/* fbput FB_DSNAME */
	retval = fbput(snd_buf, FB_DSNAME, dsload_dsname, 0);
	if (retval == -1)
	{
		fprintf(stderr, "dsload: ***An error occurred while storing DSNAME in field buffer->%s\n", fbstrerror(fberror)); 
		goto _DSLOAD_MAIN_ERR_RETURN_03;
	}
	
	/* fbput FB_MEMNAME */
	retval = fbput(snd_buf, FB_MEMNAME, dsload_member, 0);
	if (retval == -1)
	{
		fprintf(stderr, "dsload: ***An error occurred while storing MEMNAME in field buffer->%s\n", fbstrerror(fberror)); 
		goto _DSLOAD_MAIN_ERR_RETURN_03;
	}
	
	/* fbput FB_CATNAME */
	retval = fbput(snd_buf, FB_CATNAME, dsload_catalog, 0);
	if (retval == -1)
	{
		fprintf(stderr, "dsload: ***An error occurred while storing CATNAME in field buffer->%s\n", fbstrerror(fberror)); 
		goto _DSLOAD_MAIN_ERR_RETURN_03;
	}
	
	/* fbput FB_FILEPATH */
	retval = fbput(snd_buf, FB_FILEPATH, dsload_dstpath, 0);
	if (retval == -1)
	{
		fprintf(stderr, "dsload: ***An error occurred while storing FILEPATH in field buffer->%s\n", fbstrerror(fberror)); 
		goto _DSLOAD_MAIN_ERR_RETURN_03;
	}
	
	/* fbput FB_VOLUME */
	retval = fbput(snd_buf, FB_VOLUME, dsload_volser, 0);
	if (retval == -1)
	{
		fprintf(stderr, "dsload: ***An error occurred while storing VOLUME in field buffer->%s\n", fbstrerror(fberror)); 
		goto _DSLOAD_MAIN_ERR_RETURN_03;
	}
	
	/* fbput FB_ARGS */
	retval = fbput(snd_buf, FB_ARGS, dsload_delimeter, 0);
	if (retval == -1)
	{
		fprintf(stderr, "dsload: ***An error occurred while storing FB_ARGS in field buffer->%s\n", fbstrerror(fberror)); 
		goto _DSLOAD_MAIN_ERR_RETURN_03;
	}
	
	/* fbput FB_TYPE */
	if (_file_check)
	{
		retval = fbput(snd_buf, FB_TYPE, "F" , 0);
		if (retval == -1)
		{
			fprintf(stderr, "dsload: ***An error occurred while storing CATNAME in field buffer->%s\n", fbstrerror(fberror)); 
			goto _DSLOAD_MAIN_ERR_RETURN_03;
		}
	}
	
	/* tmax service call */
	retval = tpcall("OFRUISVRDSLOAD", (char *)snd_buf, 0, (char **)&rcv_buf, &rcv_len, TPNOFLAGS);
	if (retval < 0) 
	{	// an error occurred
		fprintf(stderr, "dsload: ***An error occurred in server OFRUISVRDSLOAD->%s\n", tpstrerror(tperrno));
		retval = fbget(rcv_buf, FB_RETMSG, retmsg, 0);		
		fprintf(stderr, "----------------Return message----------------\n%s\n----------------------------------------------\n", retmsg);
		goto _DSLOAD_MAIN_ERR_RETURN_03;
	}
	
	/* print log message */
	printf("Dataset is loaded successfully.\nPath: [%s]\n", dsload_dstpath);
	retval = 0;
	
_DSLOAD_MAIN_ERR_RETURN_03:
	/* release buffers */
	tpfree((char *)rcv_buf);
	tpfree((char *)snd_buf);
	
_DSLOAD_MAIN_ERR_RETURN_02:
	/* tool logout process */
	dscom_tool_logout();
	
_DSLOAD_MAIN_ERR_RETURN_01:
	/* call a function to log a record */
//	log_a_record("DSLOAD", record, retval);
	
_DSLOAD_MAIN_ERR_RETURN_00:
	/* process returns here */
	return retval;
}


int check_args(int argc, char *argv[])
{
	int i;

	/* check minimum argument count */
	if (argc < 2) return -1;
	strcpy(dsload_dsname, argv[1]);

	i = 2;
	while (argv[i]) {
		if (!strcmp(argv[i],"-c") || !strcmp(argv[i],"--catalog")) {
			if (argv[i+1]) {
				strcpy(dsload_catalog,argv[i+1]);
			} else {
				return -1;
			}
			i++;
		} else if (!strcmp(argv[i],"-v") || !strcmp(argv[i],"--volser")) {
			if (argv[i+1]) {
				strcpy(dsload_volser,argv[i+1]);
			} else {
				return -1;
			}
			i++;
		} else if (!strcmp(argv[i],"-m") || !strcmp(argv[i],"--member")) {
			if (argv[i+1]) {
				strcpy(dsload_member,argv[i+1]);
			} else {
				return -1;
			}
			i++;
		} else if (!strcmp(argv[i],"-d") || !strcmp(argv[i],"--delimiter")) {
			if (argv[i+1]) {
				if (!strcmp(argv[i+1],"NEWLINE")) strcpy(dsload_deli_form, "\\n");
				else strcpy(dsload_deli_form, argv[i+1]);
			} else {
				return -1;
			}
			i++;
		} else if (!strcmp(argv[i],"-t") || !strcmp(argv[i],"--target")) {
			if (argv[i+1]) {
				strcpy(dsload_dstpath,argv[i+1]);
			} else {
				return -1;
			}
			i++;
		} else if (!strcmp(argv[i],"-F") || !strcmp(argv[i],"--force")) {
			_file_check = 0;
		}
		i++;
	}

	return 0;
}


int print_usage()
{
	printf("Usage: dsload <dsname> [options]\n");
	printf("<dsname>            Specifies the dataset name to be loaded\n");
	printf("\n");

	printf("Options:\n");
	printf("  -v, --volser      Specifies the volume serial that contains the dataset entry\n");
	printf("  -c, --catalog     Specifies the catalog name that contains the dataset entry\n");
	printf("  -m, --member      Specifies the member name of the PDS dataset\n");
	printf("  -t, --target      Specifies the destination filepath\n");
	printf("  -d, --delimeter   Specifies the delimeter to separate each record\n");
	printf("  -F, --force       Ignores existance of the destination file\n");
	printf("\n");

	return 0;
}


int validate_param()
{
	/* check if dataset name is specified */
	if( dsload_dsname[0] != '\0' ) {
		/* check if dataset name is invalid */
		if( dscom_check_dsname(dsload_dsname) < 0 ) {
			fprintf(stderr, "dsload: *** invalid dataset name specified - dsname=%s\n", dsload_dsname);
			return -1;
		}
	}

	/* check if member name is specified */
	if( dsload_member[0] != '\0' ) {
		/* check if member name is invalid */
		if( dscom_check_dsname2(dsload_dsname, dsload_member) < 0 ) {
			fprintf(stderr, "dsload: *** invalid dataset name or member name specified - dsname=%s,member=%s\n", dsload_dsname, dsload_member);
			return -1;
		}
	}

	/* check if catalog name is specified */
	if( dsload_catalog[0] != '\0' ) {
		/* check if catalog name is invalid */
		if( dscom_check_dsname(dsload_catalog) < 0 ) {
			fprintf(stderr, "dsload: *** invalid catalog name specified - catalog=%s\n", dsload_catalog);
			return -1;
		}
	}

	/* check if volser is specified */
	if( dsload_volser[0] != '\0' ) {
		/* check if volser is invalid */
		if( dscom_check_dsload_volser(dsload_volser) < 0) {
			fprintf(stderr, "dsload: *** invalid volser specified - volser=%s\n", dsload_volser);
			return -1;
		}
	}

	return 0;
}


int adjust_param()
{
	int  retval;
	char s_temp[1024];

	/* use temp directory if filepath is not specified */
	if (dsload_dstpath[0] == 0x00) {
		retval = ofcom_conf_get_value("dstool.conf", "DSLOAD", "LOAD_DIR", s_temp, sizeof(s_temp));
		if (retval < 0) {
			fprintf(stderr, "dsload: *** Get configuration failed. dstool.conf [DSLOAD] LOAD_DIR\n");
			return -1;
		}

		strcpy(dsload_dstpath, s_temp);
		strcat(dsload_dstpath, "/");
		strcat(dsload_dstpath, dsload_dsname);

		if (dsload_member[0] != 0x00) {
			strcat(dsload_dstpath, ".");
			strcat(dsload_dstpath, dsload_member);
		}
	}

	/* use config setting if delimeter is not specified */
	if (dsload_deli_form[0] == 0x00) {
		retval = ofcom_conf_get_value("dstool.conf", "DSLOAD", "DELIMITER", s_temp, sizeof(s_temp));
		if (retval < 0) {
			strcpy(dsload_deli_form, "");
		} else {
			if (!strcmp(s_temp, "NEWLINE")) strcpy(dsload_deli_form, "\\n");
			else strcpy(dsload_deli_form, s_temp);
		}
	}

	return 0;
}


int convert_delim()
{
	int  length, i;
	char ch, *ptr;

	/* initialize delimeter */
	memset(dsload_delimeter,0x00,sizeof(dsload_delimeter));
	ptr = dsload_delimeter;

	/* convert delimeter */
	if (dsload_deli_form[0]) {
		length = strlen(dsload_deli_form);
		for( i = 0; i < length; i++ ) {
			if( dsload_deli_form[i] == '\\' && i+1 < length ) {
				i++; ch = dsload_deli_form[i];
				switch( ch ) {
					case 'a': *ptr++ = '\a'; break; /* alert character */
					case 'b': *ptr++ = '\b'; break; /* backspace */
					case 'f': *ptr++ = '\f'; break; /* formfeed */
					case 'n': *ptr++ = '\n'; break; /* newline */
					case 'r': *ptr++ = '\r'; break; /* carriage return */
					case 't': *ptr++ = '\t'; break; /* horizontal tab */
					case 'v': *ptr++ = '\v'; break; /* vertical tab */
					default : *ptr++ =  ch ; break; /* default case */
				}
			} else *ptr++ = dsload_deli_form[i];
		}
	}

	return 0;
}
