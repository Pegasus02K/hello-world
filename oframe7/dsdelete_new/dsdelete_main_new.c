/* Dataset Delete
 *
 * Description:
 *      Delete an existing dataset
 *
 * Used service:
 *      OFRUISVRDSDEL
 *
 * Parameters to service:
 *      FB_TACF_TOKEN(string): TACF token
 *      FB_DSNAME(string): dataset name
 *      FB_CATNAME(string): catalog name (optional)
 *      FB_VOLUME(string): volume serial (optional)
 *      FB_TYPE(char): delete options (optional)
 *
 * Format(FB_TYPE):
 *      I: ignore parameter validation check
 *      U: delete only the catalog entry for the dataset (uncatalog)
 *
 * Return values from service
 *      FB_RETMSG(string): error message
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <signal.h>
#include <usrinc/atmi.h>
#include <usrinc/fbuf.h>
#include <usrinc/tmaxapi.h>
#include <oframe_fdl.h>
#include "ofcom.h"
#include "dscom.h"
#include "ds.h"
#include "nvsm.h"
#include "login.h"
#include "safx.h"

#define TOOL_NAME		"dsdelete"

extern char *dsdelete_version;
extern char *dsdelete_build_info;

char dsdelete_dsname[DS_DSNAME_LEN + 2] = {0,};
char dsdelete_catalog[DS_DSNAME_LEN + 2] = {0,};
char dsdelete_volser[DS_VOLSER_LEN + 2] = {0,};
char dsdelete_member[NVSM_MEMBER_LEN + 2] = {0,};
// dataset name with member name: dsname + '(' + member + ')'
char dsdelete_dsnwmem[DS_DSNAME_LEN + NVSM_MEMBER_LEN + 2 +2] = {0,};

int dsdelete_ignore = 0;
int dsdelete_uncatalog = 0;
int dsdelete_cataloged = 0;
char tacf_token[SAFX_TOKEN_SIZE + 1];

int error_return(int error_code, char *function_name);
int check_args(int argc, char *argv[]);
int print_usage();
int validate_param();
int set_field_buffer(FBUF * fbuf);
int log_a_record(char *title, char *record, int rcode);
int tacf_login();

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
	int retval; 
	char record[1024];
//	char temprec[1024];
	
	/* TPCALL variables */
	FBUF *snd_buf = NULL;
	FBUF *rcv_buf = NULL;
	long rcv_len;
	char retmsg[1024];

	/* print version and one line summary */
	if( argc >= 2 && ! strcmp(argv[1], "-V") )
	{
		printf("dsdelete version %s %s\n", dsdelete_version+26, dsdelete_build_info+22);
		fflush(stdout); return 0;
	} 
	else 
	{
		printf("dsdelete version %s %s\n", dsdelete_version+26, dsdelete_build_info+22);
		printf("Delete an Existing Dataset or a Member of PDS Dataset\n\n"); fflush(stdout);
	}

	/* check input arguments */
	retval = check_args(argc, argv);
	if( retval < 0 ) { print_usage(); return -1; }

	/* validate parameters */
	retval = validate_param();
	if( retval < 0 ) goto _DSDELETE_MAIN_ERR_RETURN_00;
	
	/* compose trace log record */
	sprintf(record, "DSNAME=%s,CATALOG=%s,VOLSER=%s,MEMBER=%s", dsdelete_dsname, dsdelete_catalog, dsdelete_volser, dsdelete_member);
	
	/* print a log message */
	printf("DSDELETE %s\n", record); fflush(stdout);
	retval = 0;

	/* TACF login process */
	retval = tacf_login();
	if (retval < 0) goto _DSDELETE_MAIN_ERR_RETURN_01;
	
	/* catch signals */
	if ( signal(SIGINT, _signal_handler) == SIG_ERR )
	{
		fprintf(stderr, "Cannot handle SIGINT!");
		exit(EXIT_FAILURE);
	}
	if ( signal(SIGTERM, _signal_handler) == SIG_ERR)  
	{
		fprintf(stderr, "Cannot handle SIGTERM!");
		exit(EXIT_FAILURE);
	}
	
	/* initialize user return code */
	tpurcode = 0;
	
	/* allocate send buffer */
	snd_buf = (FBUF *)tpalloc( "FIELD", NULL, 0);
	if ( snd_buf == NULL )
	{
		fprintf(stderr, "dsdelete: ***Couldn't allocate send buffer->%s\n",tpstrerror(tperrno)); 
		goto _DSDELETE_MAIN_ERR_RETURN_02;
	}
	
	/* allocate receive buffer */
	rcv_buf = (FBUF *)tpalloc("FIELD", NULL, 0);
	if ( rcv_buf == NULL )
	{
		fprintf(stderr, "dsdelete: ***Couldn't allocate send buffer->%s\n",tpstrerror(tperrno)); 
		tpfree((char *)snd_buf);
		goto _DSDELETE_MAIN_ERR_RETURN_02;
	}
	
	retval = set_field_buffer(snd_buf);
	if (retval < 0)
		goto _DSDELETE_MAIN_ERR_RETURN_03;
	
	/* tmax service call */
	retval = tpcall("OFRUISVRDSDEL", (char *)snd_buf, 0, (char **)&rcv_buf, &rcv_len, TPNOFLAGS);
	if (retval < 0) 
	{	// an error occurred
		fprintf(stderr, "dsdelete: ***An error occurred in server OFRUISVRDSDEL->%s\n", tpstrerror(tperrno));
		retval = fbget(rcv_buf, FB_RETMSG, retmsg, 0);		
		fprintf(stderr, "----------------Return message----------------\n%s\n----------------------------------------------\n", retmsg);
		goto _DSDELETE_MAIN_ERR_RETURN_03;
	}
	
	/* print a log message */
	printf("COMPLETED SUCCESSFULLY.\n"); fflush(stdout);
	retval = 0;
	
_DSDELETE_MAIN_ERR_RETURN_03:
	/* release buffers */
	tpfree((char *)rcv_buf);
	tpfree((char *)snd_buf);

_DSDELETE_MAIN_ERR_RETURN_02:
	/* TACF logout */
	logout_tacf_token(tacf_token, 0);

_DSDELETE_MAIN_ERR_RETURN_01:
	/* call a function to log a record */
	log_a_record("DSDELETE", record, retval);

_DSDELETE_MAIN_ERR_RETURN_00:
	/* process returns here */
	return retval;
}


int error_return(int error_code, char *function_name)
{
	fprintf(stderr, "dsdelete: *** %s failed - errcode=%d\n", function_name, error_code);
	return error_code;
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
			strcpy(dsdelete_catalog, argv[i]);

		/* check if -v option is specified */
		} else if( ! strcmp(argv[i], "-v") || ! strcmp(argv[i], "--volume") ) {
			SKIP_ARGI_OR_RETURN_ERROR(i, argc);
			if( strlen(argv[i]) > DS_VOLSER_LEN ) return -1;
			strcpy(dsdelete_volser,  argv[i]);

		/* check if -m option is specified */
		} else if( ! strcmp(argv[i], "-m") || ! strcmp(argv[i], "--member") ) {
			SKIP_ARGI_OR_RETURN_ERROR(i, argc);
/*
			if( dscom_is_relaxed_member_limit() ) {
				if( strlen(argv[i]) > NVSM_MEMBER_LEN ) return -1;
			} else {
				if( strlen(argv[i]) > DS_MEMBER_LEN ) return -1;
			}
*/
			strcpy(dsdelete_member,  argv[i]);


		/* check if -I option is specified */
		} else if( ! strcmp(argv[i], "-I") || ! strcmp(argv[i], "--ignore") ) {
			dsdelete_ignore = 1;

		/* check if -U option is specified */
		} else if( ! strcmp(argv[i], "-U") || ! strcmp(argv[i], "--uncatalog") ) {
			dsdelete_uncatalog = 1;

		/* otherwise <dsname> specified */
		} else {
			if( argv[i][0] == '-' ) return -1;
			if( strlen(argv[i]) > DS_DSNAME_LEN ) return -1;

			if( dsdelete_dsname[0] ) return -1;
			strcpy(dsdelete_dsname, argv[i]);
		}
	}

	/* check if the dataset name is not specified */
	if( ! dsdelete_dsname[0] ) return -1;

	return 0;
}


int print_usage()
{
	printf("Usage: dsdelete [options] <dsname>\n");
	printf("<dsname>            Specifies the dataset name to be deleted\n");
	printf("\n");

	printf("Options:\n");
	printf("  -c, --catalog     Specifies the catalog name that contains the dataset entry\n");
	printf("  -v, --volume      Specifies the volume serial where the dataset to be found\n");
	printf("  -m, --member      Specifies the member name of the PDS dataset\n");
	printf("  -I, --ignore      Ignores parameter validation check errors and tries to proceed\n");
	printf("  -U, --uncatalog   Deletes only the catalog entry for the dataset\n");
	printf("\n");

	return 0;
}


int validate_param()
{
	/* check if dataset name is specified */
	if( dsdelete_dsname[0] != '\0' )
	{
		/* check if dataset name is invalid */
		if( dscom_check_dsname(dsdelete_dsname) < 0 )
		{
			fprintf(stderr, "dsdelete: *** invalid dataset name specified - dsname=%s\n", dsdelete_dsname);
			if( ! dsdelete_ignore ) return -1; /* return negative if ignore option is not specified */
		}
	}

	/* check if catalog name is specified */
	if( dsdelete_catalog[0] != '\0' )
	{
		/* check if catalog name is invalid */
		if( dscom_check_dsname(dsdelete_catalog) < 0 )
		{
			fprintf(stderr, "dsdelete: *** invalid catalog name specified - catalog=%s\n", dsdelete_catalog);
			if( ! dsdelete_ignore ) return -1; /* return negative if ignore option is not specified */
		}
	}

	/* check if volume serial is specified */
	if( dsdelete_volser[0] != '\0' )
	{
		/* check if volume serial is invalid */
		if( dscom_check_volser(dsdelete_volser) < 0 )
		{
			fprintf(stderr, "dsdelete: *** invalid volume serial specified - volser=%s\n", dsdelete_volser);
			if( ! dsdelete_ignore ) return -1; /* return negative if ignore option is not specified */
		}
	}

	/* check if member name is specified */
	if( dsdelete_member[0] != '\0' )
	{
		/* check if member name is invalid */
		if( dscom_check_dsname2(dsdelete_dsname, dsdelete_member) < 0 )
		{
			fprintf(stderr, "dsdelete: *** invalid dataset name or member name specified - dsname=%s,member=%s\n", dsdelete_dsname, dsdelete_member);
			if( ! dsdelete_ignore ) return -1; /* return negative if ignore option is not specified */
		}
	}

	return 0;
}


int set_field_buffer(FBUF * fbuf)
{
	int retval;

	/* fbput FB_TACF_TOKEN */
	retval = fbput(fbuf, FB_TACF_TOKEN, tacf_token, 0);
	if ( retval == -1 )
	{
		fprintf(stderr, "dsdelete: ***An error occurred while storing TACF_TOKEN in field buffer->%s\n", fbstrerror(fberror)); 
		return -1;
	}
	
	/* fbput FB_DSNAME */
	/* if member name is specified, put dsdelete_dsnwmem */
	if( dsdelete_member[0] != '\0' )
	{
		strcat(dsdelete_dsnwmem, dsdelete_dsname);
		strcat(dsdelete_dsnwmem, "(");
		strcat(dsdelete_dsnwmem, dsdelete_member);
		strcat(dsdelete_dsnwmem, ")");
		
		retval = fbput(fbuf, FB_DSNAME, dsdelete_dsnwmem, 0);
	}
	else
		retval = fbput(fbuf, FB_DSNAME, dsdelete_dsname, 0);
	if ( retval == -1 )
	{
		fprintf(stderr, "dsdelete: ***An error occurred while storing DSNAME in field buffer->%s\n", fbstrerror(fberror)); 
		return -1;
	}

	/* fbput FB_CATNAME */
	retval = fbput(fbuf, FB_CATNAME, dsdelete_catalog, 0);
	if ( retval == -1 )
	{
		fprintf(stderr, "dsdelete: ***An error occurred while storing CATNAME in field buffer->%s\n", fbstrerror(fberror)); 
		return -1;
	}

	/* fbput FB_VOLUME */
	retval = fbput(fbuf, FB_VOLUME, dsdelete_volser, 0);
	if ( retval == -1 )
	{
		fprintf(stderr, "dsdelete: ***An error occurred while storing VOLUME in field buffer->%s\n", fbstrerror(fberror)); 
		return -1;
	}

	/* fbput FB_TYPE */
	if ( dsdelete_uncatalog )
	{
		retval = fbput(fbuf, FB_TYPE, "u", 0);
		if (retval == -1)
		{
			fprintf(stderr, "dsdelete: ***An error occurred while storing TYPE(uncatalog option) in field buffer->%s\n", fbstrerror(fberror)); 
			return -1;
		}
	}
	if ( dsdelete_ignore )
	{
		retval = fbput(fbuf, FB_TYPE, "I", 0);
		if (retval == -1)
		{
			fprintf(stderr, "dsdelete: ***An error occurred while storing TYPE(ignore option) in field buffer->%s\n", fbstrerror(fberror)); 
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


int tacf_login()
{
	int retval;
	char *tjesmgr_loginfo = {0,};
	
	int login_flag = 0;
	int tacf_flag  = 0;
	safl_config_t config;
  
	/* set tacfmgr config */
	strcpy(config.file_name, "tjesmgr.conf");
	strcpy(config.section, "DEFAULT_USER");
	strcpy(config.entry_userid, "USERNAME");
	strcpy(config.entry_grpname, "GROUPNAME");
	strcpy(config.entry_passwd, "PASSWORD" );
	strcpy(config.entry_enpasswd, "ENPASSWD" );
	
	/* try TACF Login */
	retval = login_tacf_token(tjesmgr_loginfo, &config, tacf_token, login_flag, tacf_flag);
	if ( retval < 0 ) 
	{
		fprintf(stderr, "%s: *** TACF Login failed->errcode[%d]\n", TOOL_NAME, retval);
		return -1;
	}
	
	return 0;
}
