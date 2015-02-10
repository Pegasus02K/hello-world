#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <ctype.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <usrinc/atmi.h>
#include <usrinc/fbuf.h>
#include <usrinc/tmaxapi.h>
#include <oframe_fdl.h>
#include "ofcom.h"
#include "ds.h"
#include "dscom.h"
#include "volm.h"
#include "ams.h"
#include "dsalc.h"
#include "dsio_batch.h"


extern char *dscreate_version;
extern char *dscreate_build_info;


char dscreate_dsname[DS_DSNAME_LEN + 2] = {0,};

char dscreate_catalog[DS_DSNAME_LEN + 2] = {0,};
char dscreate_volser[DS_VOLSER_LEN + 2] = {0,};
char dscreate_unit[DS_UNIT_LEN + 2] = {0,};
char dscreate_member[NVSM_MEMBER_LEN + 2] = {0,};

char dscreate_dsorg[8] = {0,};
char dscreate_recfm[8] = {0,};

int32_t dscreate_blksize = 0;
int16_t dscreate_lrecl = 0;
int16_t dscreate_keylen = 0;
int16_t dscreate_keypos = 0;

char dscreate_space[256] = {0,};
char dscreate_expdt[8+2] = {0,};
int dscreate_retpd = 0;

int dscreate_nocatalog = 0;
int dscreate_recatalog = 0;

char dscreate_args[1024] = {0,};
char dscreate_type = 0;

char dscreate_nocatalog_to_svr[4] = {0,};
char dscreate_primary[100] = {0,};
char dscreate_secondary[100] = {0,};
char dscreate_directory[100] = {0,};
char dscreate_avgval[100] = {0,};


int error_return(int error_code, char *function_name);

int check_args(int argc, char *argv[]);
int print_usage();

int validate_param();

int check_ds_name();
int log_a_record(char *title, char *record, int rcode);

int process_space_param(char *param);

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
	if(argc >= 2 && ! strcmp(argv[1], "-V"))
	{
		printf("dscreate version %s %s\n", dscreate_version+26, dscreate_build_info+22);
		fflush(stdout); return 0;
	} 
	else
	{
		printf("dscreate version %s %s\n", dscreate_version+26, dscreate_build_info+22);
		printf("Create a New Dataset or a Member of PDS Dataset\n\n"); fflush(stdout);
	}

	/* check input arguments */
	retval = check_args(argc, argv);
	if(retval < 0) { print_usage(); return -1; }

	/* validate parameters */
	retval = validate_param();
	if(retval < 0) goto _DSCREATE_MAIN_ERR_RETURN_00;

	/* compose trace log record */
	sprintf(record, "DSNAME=%s,CATALOG=%s,VOLSER=%s,MEMBER=%s",
		dscreate_dsname, dscreate_catalog, dscreate_volser, dscreate_member);

	/* print a log message */
	printf("DSCREATE %s\n", record); fflush(stdout);
	retval = 0;

	/* tool login process */
	retval = dscom_tool_login("DSCREATE");
	if(retval < 0) goto _DSCREATE_MAIN_ERR_RETURN_01;

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
	
	/* check dataset name */
	retval = check_ds_name();
	if(retval < 0) goto _DSCREATE_MAIN_ERR_RETURN_03;
	
	if(dscreate_nocatalog)
	{
		strcpy(dscreate_nocatalog_to_svr, "YES");
		dscreate_type = 'N';
	}
	else if (dscreate_recatalog)
		dscreate_type = 'R';
	
	retval = process_space_param(dscreate_space);
	if(retval < 0)
	{
		fprintf(stderr, "dscreate: *** invalid space parameter - SPACE=%s\n", dscreate_space);
		return -1;
	}
	
//TODO: directory should be added here
	sprintf(dscreate_args, "%s;%s;%s;%s;%s;%d;%d;%s;%s;%d;%d;%s;%s;%s;%s;%s;",
			dscreate_member, dscreate_volser, dscreate_dsorg, dscreate_recfm, dscreate_unit, dscreate_lrecl,
			dscreate_blksize, dscreate_expdt, dscreate_catalog, dscreate_keylen,
			dscreate_keypos, dscreate_nocatalog_to_svr, dscreate_primary, dscreate_secondary, dscreate_avgval, dscreate_directory);

	/* initialize user return code */
	tpurcode = 0;
	
	/* allocate send buffer */
	snd_buf = (FBUF *)tpalloc( "FIELD", NULL, 0);
	if(snd_buf == NULL)
	{
		fprintf(stderr, "dscreate: ***Couldn't allocate send buffer->%s\n",tpstrerror(tperrno)); 
		goto _DSCREATE_MAIN_ERR_RETURN_02;
	}
	
	/* allocate receive buffer */
	rcv_buf = (FBUF *)tpalloc("FIELD", NULL, 0);
	if(rcv_buf == NULL)
	{
		fprintf(stderr, "dscreate: ***Couldn't allocate send buffer->%s\n",tpstrerror(tperrno)); 
		tpfree((char *)snd_buf);
		goto _DSCREATE_MAIN_ERR_RETURN_02;
	}
	
	/* fbput FB_DSNAME */
	retval = fbput(snd_buf, FB_DSNAME, dscreate_dsname, 0);
	if (retval == -1)
	{
		fprintf(stderr, "dscreate: ***An error occurred while storing DSNAME in field buffer->%s\n", fbstrerror(fberror)); 
		goto _DSCREATE_MAIN_ERR_RETURN_03;
	}
	
	/* fbput FB_ARGS */
	retval = fbput(snd_buf, FB_ARGS, dscreate_args, 0);
	if (retval == -1)
	{
		fprintf(stderr, "dscreate: ***An error occurred while storing ARGS in field buffer->%s\n", fbstrerror(fberror)); 
		goto _DSCREATE_MAIN_ERR_RETURN_03;
	}
	
	/* fbput FB_TYPE */
	if (dscreate_type)
		retval = fbput(snd_buf, FB_TYPE, (char*)&dscreate_type, 0);
	if (retval == -1)
	{
		fprintf(stderr, "dscreate: ***An error occurred while storing TYPE in field buffer->%s\n", fbstrerror(fberror)); 
		goto _DSCREATE_MAIN_ERR_RETURN_03;
	}

	/* create a new dataset: tmax service call */
	retval = tpcall("OFRUISVRDSCRE", (char *)snd_buf, 0, (char **)&rcv_buf, &rcv_len, TPNOFLAGS);
	if (retval < 0) 
	{	// an error occurred
		fprintf(stderr, "dscreate: ***An error occurred in server OFRUISVRDSCRE->%s\n", tpstrerror(tperrno));
		retval = fbget(rcv_buf, FB_RETMSG, retmsg, 0);		
		fprintf(stderr, "----------------Return message----------------\n%s----------------------------------------------\n", retmsg);
		goto _DSCREATE_MAIN_ERR_RETURN_03;
	}
	
	/* print a log message */
	printf("COMPLETED SUCCESSFULLY.\n"); fflush(stdout);
	retval = 0;

_DSCREATE_MAIN_ERR_RETURN_03:
	/* release buffers */
	tpfree((char *)rcv_buf);
	tpfree((char *)snd_buf);

_DSCREATE_MAIN_ERR_RETURN_02:
	/* tool logout process */
	dscom_tool_logout();

_DSCREATE_MAIN_ERR_RETURN_01:
	/* call a function to log a record */
	log_a_record("DSCREATE", record, retval);

_DSCREATE_MAIN_ERR_RETURN_00:
	/* process returns here */
	return retval;
}


int error_return(int error_code, char *function_name)
{
	fprintf(stderr, "dscreate: *** %s failed - errcode=%d\n", function_name, error_code);
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
			strcpy(dscreate_catalog, argv[i]);

		/* check if -v option is specified */
		} else if( ! strcmp(argv[i], "-v") || ! strcmp(argv[i], "--volume") ) {
			SKIP_ARGI_OR_RETURN_ERROR(i, argc);
			if( strlen(argv[i]) > DS_VOLSER_LEN ) return -1;
			strcpy(dscreate_volser, argv[i]);

		/* check if -u option is specified */
		} else if( ! strcmp(argv[i], "-u") || ! strcmp(argv[i], "--unit") ) {
			SKIP_ARGI_OR_RETURN_ERROR(i, argc);
			if( strlen(argv[i]) > DS_UNIT_LEN ) return -1;
			strcpy(dscreate_unit, argv[i]);

		/* check if -m option is specified */
		} else if( ! strcmp(argv[i], "-m") || ! strcmp(argv[i], "--member") ) {
			SKIP_ARGI_OR_RETURN_ERROR(i, argc);
			if( dscom_is_relaxed_member_limit() ) {  
				if( strlen(argv[i]) > NVSM_MEMBER_LEN ) return -1;
			} else {
				if( strlen(argv[i]) > DS_MEMBER_LEN ) return -1;
			}
			strcpy(dscreate_member, argv[i]);

		/* check if -o option is specified */
		} else if( ! strcmp(argv[i], "-o") || ! strcmp(argv[i], "--dsorg") ) {
			SKIP_ARGI_OR_RETURN_ERROR(i, argc);
			if( strlen(argv[i]) > 4 ) return -1;
			strcpy(dscreate_dsorg, argv[i]);

		/* check if -f option is specified */
		} else if( ! strcmp(argv[i], "-f") || ! strcmp(argv[i], "--recfm") ) {
			SKIP_ARGI_OR_RETURN_ERROR(i, argc);
			if( strlen(argv[i]) > 4 ) return -1;
			strcpy(dscreate_recfm, argv[i]);

		/* check if -b option is specified */
		} else if( ! strcmp(argv[i], "-b") || ! strcmp(argv[i], "--blksize") ) {
			SKIP_ARGI_OR_RETURN_ERROR(i, argc);
			if( dscom_check_isdigit(argv[i]) < 0 ) return -1;
			dscreate_blksize = atoi(argv[i]);

		/* check if -l option is specified */
		} else if( ! strcmp(argv[i], "-l") || ! strcmp(argv[i], "--lrecl") ) {
			SKIP_ARGI_OR_RETURN_ERROR(i, argc);
			if( dscom_check_isdigit(argv[i]) < 0 ) return -1;
			dscreate_lrecl = atoi(argv[i]);

		/* check if -k option is specified */
		} else if( ! strcmp(argv[i], "-k") || ! strcmp(argv[i], "--keylen") ) {
			SKIP_ARGI_OR_RETURN_ERROR(i, argc);
			if( dscom_check_isdigit(argv[i]) < 0 ) return -1;
			dscreate_keylen = atoi(argv[i]);

		/* check if -p option is specified */
		} else if( ! strcmp(argv[i], "-p") || ! strcmp(argv[i], "--keypos") ) {
			SKIP_ARGI_OR_RETURN_ERROR(i, argc);
			if( dscom_check_isdigit(argv[i]) < 0 ) return -1;
			dscreate_keypos = atoi(argv[i]);

		/* check if -s option is specified */
		} else if( ! strcmp(argv[i], "-s") || ! strcmp(argv[i], "--space") ) {
			SKIP_ARGI_OR_RETURN_ERROR(i, argc);
			if( strlen(argv[i]) > 255 ) return -1;
			strcpy(dscreate_space, argv[i]);

		/* check if -x option is specified */
		} else if( ! strcmp(argv[i], "-x") || ! strcmp(argv[i], "--expdt") ) {
			SKIP_ARGI_OR_RETURN_ERROR(i, argc);
			if( strlen(argv[i]) != 8 ) return -1;
			strcpy(dscreate_expdt, argv[i]);

		/* check if -r option is specified */
		} else if( ! strcmp(argv[i], "-r") || ! strcmp(argv[i], "--retpd") ) {
			SKIP_ARGI_OR_RETURN_ERROR(i, argc);
			dscreate_retpd = atoi(argv[i]);

		/* check if -N option is specified */
		} else if( ! strcmp(argv[i], "-N") || ! strcmp(argv[i], "--nocatalog") ) {
			if( dscreate_recatalog ) return -1;
			dscreate_nocatalog = 1;

		/* check if -R option is specified */
		} else if( ! strcmp(argv[i], "-R") || ! strcmp(argv[i], "--recatalog") ) {
			if( dscreate_nocatalog ) return -1;
			dscreate_recatalog = 1;

		/* otherwise <dsname> specified */
		} else {
			if( argv[i][0] == '-' ) return -1;
			if( strlen(argv[i]) > DS_DSNAME_LEN ) return -1;

			if( dscreate_dsname[0] ) return -1;
			strcpy(dscreate_dsname, argv[i]);
		}
	}

	/* check if the dataset name is not specified */
	if( ! dscreate_dsname[0] ) return -1;

	return 0;
}


int print_usage()
{
	printf("Usage: dscreate [options] <dsname>\n");
	printf("<dsname>			Specifies the dataset name to be created\n");
	printf("\n");

	printf("Options:\n");
	printf("  -c, --catalog	 Specifies the catalog name in which the dataset to be defined\n");
	printf("  -v, --volume	  Specifies the volume serial to contain the dataset\n");
	printf("  -u, --unit		Specifies the unit type to which a dataset to be allocated\n");
	printf("  -m, --member	  Specifies the member name of the PDS dataset\n");
	printf("  -o, --dsorg	   Specifies the dataset organization\n");
	printf("  -f, --recfm	   Specifies the record format\n");
	printf("  -b, --blksize	 Specifies the block size\n");
	printf("  -l, --lrecl	   Specifies the record length\n");
	printf("  -k, --keylen	  Specifies the key length\n");
	printf("  -p, --keypos	  Specifies the key position\n");
	printf("  -s, --space	   Specifies the space parameter (TRK/CYL/unit,primary,second,directory)\n");
	printf("  -x, --expdt	   Specifies the expiration date (in YYYYMMDD format)\n");
	printf("  -r, --retpd	   Specifies the retention period (number of days)\n");
	printf("  -N, --nocatalog   Do not catalog the dataset newly created\n");
	printf("  -R, --recatalog   Define only the catalog entry for the dataset\n");
	printf("\n");

	return 0;
}


int validate_param()
{
	/* check if dataset name is specified */
	if( dscreate_dsname[0] != '\0' ) {
		/* check if dataset name is invalid */
		if( dscom_check_dsname(dscreate_dsname) < 0 ) {
			fprintf(stderr, "dscreate: *** invalid dataset name specified - dsname=%s\n", dscreate_dsname);
			return -1;
		}
	}

	/* check if member name is specified */
	if( dscreate_member[0] != '\0' ) {
		/* check if member name is invalid */
		if( dscom_check_dsname2(dscreate_dsname, dscreate_member) < 0 ) {
			fprintf(stderr, "dscreate: *** invalid dataset name or member name specified - dsname=%s,member=%s\n", dscreate_dsname, dscreate_member);
			return -1;
		}
	}

	/* check if catalog name is specified */
	if( dscreate_catalog[0] != '\0' ) {
		/* check if catalog name is invalid */
		if( dscom_check_dsname(dscreate_catalog) < 0 ) {
			fprintf(stderr, "dscreate: *** invalid catalog name specified - catalog=%s\n", dscreate_catalog);
			return -1;
		}
	}

	/* check if volume serial is specified */
	if( dscreate_volser[0] != '\0' ) {
		/* check if volume serial is invalid */
		if( dscom_check_volser(dscreate_volser) < 0 ) {
			fprintf(stderr, "dscreate: *** invalid volume serial specified - volser=%s\n", dscreate_volser);
			return -1;
		}
	}

	/* check if unit name is specified */
	if( dscreate_unit[0] != '\0' ) {
		/* check if unit name is invalid */
		if( dscom_check_unit(dscreate_unit) < 0 ) {
			fprintf(stderr, "dscreate: *** invalid unit name specified - unit=%s\n", dscreate_unit);
			return -1;
		}
	}

	/* check if dataset organization is specified */
	if( dscreate_dsorg[0] != '\0' ) {
		/* check if dataset organization is invalid */
		if( dscom_check_dsorg(dscreate_dsorg) < 0 ) {
			fprintf(stderr, "dscreate: *** invalid dataset organization specified - dsorg=%s\n", dscreate_dsorg);
			return -1;
		}
	}

	/* check if record format is specified */
	if( dscreate_recfm[0] != '\0' ) {
		/* check if record format is invalid */
		if( dscom_check_recfm(dscreate_recfm) < 0 ) {
			fprintf(stderr, "dscreate: *** invalid record format specified - recfm=%s\n", dscreate_recfm);
			return -1;
		}
	}

	/* check if block size is specified */
	if( dscreate_blksize != 0 ) {
		/* check if block size is invalid */
		if( dscom_check_blksize(dscreate_blksize) < 0 ) {
			fprintf(stderr, "dscreate: *** invalid block size specified - blksize=%d\n", dscreate_blksize);
			return -1;
		}
	}

	/* check if record length is specified */
	if( dscreate_lrecl != 0 ) {
		/* check if record length is invalid */
		if( dscom_check_lrecl(dscreate_lrecl) < 0 ) {
			fprintf(stderr, "dscreate: *** invalid record length specified - lrecl=%d\n", dscreate_lrecl);
			return -1;
		}
	}

	/* check if key length is specified */
	if( dscreate_keylen != 0 ) {
		/* check if key length is invalid */
		if( dscom_check_keylen(dscreate_keylen) < 0 ) {
			fprintf(stderr, "dscreate: *** invalid key length specified - keylen=%d\n", dscreate_keylen);
			return -1;
		}

		/* check if key offset is invalid */
		if( dscom_check_keyoff(dscreate_keypos) < 0 ) {
			fprintf(stderr, "dscreate: *** invalid key position specified - keypos=%d\n", dscreate_keypos);
			return -1;
		}

		/* check if key length + offset exceed record length */
		if( dscreate_keylen < 0 || dscreate_lrecl < dscreate_keylen + dscreate_keypos ) {
			fprintf(stderr, "dscreate: *** invalid key info specified - lrecl=%d,keylen=%d,keypos=%d\n", dscreate_lrecl, dscreate_keylen, dscreate_keypos);
			return -1;
		}

	/* otherwise key length is not specified */
	} else {
		/* check if dataset organization is IS */
		if( ! strncmp(dscreate_dsorg, "IS", 2) ) { 
			fprintf(stderr, "dscreate: *** key length must be specified for ISAM dataset.\n");
			return -1;
		}

		/* check if key position is specified */
		if( dscreate_keypos > 0 ) {
			fprintf(stderr, "dscreate: *** key position must be specified with key length.\n");
			return -1;
		}
	}

	/* check if expiration date is specified */
	if( dscreate_expdt[0] != '\0' ) {
		/* check if expiration date is invalid */
		if( dscom_check_expdt_yyyymmdd(dscreate_expdt) < 0 ) {
			fprintf(stderr, "dscreate: *** invalid expiration date specified - expdt=%s\n", dscreate_expdt);
			return -1;
		}
	}

	/* check if retention period is specified */
	if( dscreate_retpd != 0 ) {
		/* check if retention period is invalid */
		if( dscom_check_retpd(dscreate_retpd) < 0 ) {
			fprintf(stderr, "dscreate: *** invalid retention period specified - retpd=%d\n", dscreate_retpd);
			return -1;
		}
	}

	return 0;
}


int check_ds_name()
{
	int retval;
	char dsname_original[64];

	/* check dataset name */
	retval = dscom_check_dsname(dscreate_dsname);
	if( retval < 0 ) return error_return(retval, "dscom_check_dsname()");

	/* check dsname type */
	if( retval == DSCOM_DSN_TYPE_PDS_MEMBER ) {
		strcpy(dsname_original, dscreate_dsname);
		dscom_resolve_dsname(dsname_original, dscreate_dsname, dscreate_member);
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


int process_space_param(char *param)
{
	int retval, length, pcount;
	char _copy[256], buffer[64];

	/* check if parameter is specified */
	if( ! param || ! (length = strlen(param)) ) return 0;

	/* check parenthesis */
	if( param[0] == '(' ) {
		/* parenthesis not matching */
		if( param[length-1] != ')' ) return -1;

		/* remove parenthesis */
		strcpy( _copy, param ); _copy[length-1] = '\0';
		param = _copy + 1; length = length - 2;
	}

	retval = ofcom_param_get_count(param, & pcount);
	if( retval < 0 ) return retval;

	/* param count is less than 2 */
	if( pcount < 2 ) return -1;

	retval = ofcom_param_value_by_pos(param, 0, buffer, sizeof(buffer));
	if( retval < 0 ) return retval;

	/* check avgal */
	if( buffer[0] ) {
		/* check if avgval is invalid */
		if( dscom_check_avgval(buffer) < 0 ) {
			fprintf(stderr, "dscreate: *** invalid TRK/CYL/blklth value. - TRK/CYL/blklgth=%s\n", buffer);
			return -1;
		}
	}

	if( isdigit(buffer[0]) || (!strcmp(buffer, "CYL")) || (!strcmp(buffer, "TRK")) )
		strcpy(dscreate_avgval, buffer);
	else return -1;

	retval = ofcom_param_value_by_pos(param, 1, buffer, sizeof(buffer));
	if( retval == OFCOM_ERR_SUCCESS ) strcpy( dscreate_primary ,buffer);

	retval = ofcom_param_value_by_pos(param, 2, buffer, sizeof(buffer));
	if( retval == OFCOM_ERR_SUCCESS ) strcpy(dscreate_secondary, buffer);

	retval = ofcom_param_value_by_pos(param, 3, buffer, sizeof(buffer));
	if( retval == OFCOM_ERR_SUCCESS ) strcpy(dscreate_directory, buffer);

	if(dscreate_avgval[0] == '\0' && dscreate_primary[0] != '\0')
		strcpy(dscreate_avgval, "1024");

	return 0;
}
