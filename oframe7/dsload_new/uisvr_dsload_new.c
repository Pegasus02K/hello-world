#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>

#include <usrinc/atmi.h>
#include <usrinc/fbuf.h>

#include "uisvr.h"
#include "ofcom.h"
#include "svrcom.h"
#include "ams.h"
#include "dsio_batch.h"
#include "dsalc.h"

#include "oframe_fdl.h"
#include "msgcode_uisvr.h"

/*
 * Service Name:
 *      OFRUISVRDSLOAD
 *
 * Description:
 *      Dataset load program for external editor.
 *
 * Input:
 *      FB_TACF_TOKEN(string): TACF token
 *      FB_DSNAME(string): dataset name
 *      FB_MEMNAME(string): member name
 *      FB_CATNAME(string): catalog name
 *      FB_FILEPATH(string): dataset filepath
 *      FB_TYPE(char): load options (multiple, optional)
 *
 *      FB_VOLUME(string): volume serial (optional)
 *      FB_ARGS(string): delimeter from tool
 *
 * Format(FB_TYPE):
 *      L: use the delimiter to separate each record
 *      F: ignore existance of the destination file
 *
 * Output:
 *      FB_RETMSG(string): error message
 */

#define SERVICE_NAME    "OFRUISVRDSLOAD"


static char s_dsname[DS_DSNAME_LEN + 2];
static char s_member[DS_MEMBER_LEN2 + 2];
static char s_catalog[DS_DSNAME_LEN + 2];
static char s_dstpath[255 + 1];
static char deli_form[256], delimiter[256];

//added to handle -v option
static char s_volser[DS_VOLSER_LEN + 2];

static int _file_check = 1;

static int _get_params(FBUF *rcv_buf);
static int _validate_param(char *ret_msg);

static int _adjust_param();
static int _convert_delim();

static int _init_libraries();
static int _final_libraries();

static int _check_src_exist(char *ret_msg);
static int _check_dst_exist(char *ret_msg);
static int _dsload_dataset(char *ret_msg);

static int _check_dataset_size(char *dsname, dsio_dcb_t **dcbs, char *ret_msg);
static int _dsload_dataset_inner(int fdin, int fdout, int maxlen, char *delim);


void OFRUISVRDSLOAD(TPSVCINFO *tpsvcinfo)
{
    int     retval;
    FBUF   *rcv_buf = NULL;
    FBUF   *snd_buf = NULL;
    char    ret_msg[1024] = {'\0',};

    /* service start */
    retval = svrcom_svc_start(SERVICE_NAME);
    if (retval != SVRCOM_ERR_SUCCESS) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_SVRCOM_FUNCTION_ERROR, SERVICE_NAME, "svrcom_svc_start", retval);
        goto _DSLOAD_MAIN_ERR_TPFAIL_00;
    }

    /* get rcv buffer */
    rcv_buf = (FBUF *)(tpsvcinfo->data);
    fbprint(rcv_buf);
    
    /* allocate snd buffer */
    snd_buf = (FBUF *)tpalloc("FIELD", NULL, 0);
    if (snd_buf == NULL) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_TMAX_TPALLOC_ERROR, "snd_buf", SERVICE_NAME, tpstrerror(tperrno));
        retval = SVRCOM_ERR_TPALLOC; goto _DSLOAD_MAIN_ERR_TPFAIL_00;
    }

    /* initialize parameter */
    memset(s_dsname,0x00,sizeof(s_dsname));
    memset(s_member,0x00,sizeof(s_member));
    memset(s_catalog,0x00,sizeof(s_catalog));
    memset(s_dstpath,0x00,sizeof(s_dstpath));
    memset(deli_form,0x00,sizeof(deli_form));
      
    memset(s_volser,0x00,sizeof(s_volser));
   
    /* get parameters */
    retval = _get_params(rcv_buf);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_INTERNAL_FUNC_ERROR, SERVICE_NAME, "_get_params", retval);
        sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "_get_params", retval);
        goto _DSLOAD_MAIN_ERR_TPFAIL_01;
    }

	/* validate parameters */
	retval = _validate_param(ret_msg);
	if (retval < 0 ) {
		OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_INTERNAL_FUNC_ERROR, SERVICE_NAME, "_validate_param", retval);
		goto _DSLOAD_MAIN_ERR_TPFAIL_01;
	}

    /* adjust parameters */
    retval = _adjust_param();
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_INTERNAL_FUNC_ERROR, SERVICE_NAME, "_adjust_param", retval);
        sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "_adjust_param", retval);
        goto _DSLOAD_MAIN_ERR_TPFAIL_01;
    }

    /* print load parameters */
    OFCOM_MSG_PRINTF2(UISVR_MSG_SOURCE_DATASET, SERVICE_NAME, s_dsname);
    OFCOM_MSG_PRINTF2(UISVR_MSG_SOURCE_MEMBER, SERVICE_NAME, s_member);
    OFCOM_MSG_PRINTF2(UISVR_MSG_USER_CATALOG, SERVICE_NAME, s_catalog);
    OFCOM_MSG_PRINTF2(UISVR_MSG_DEST_FILEPATH, SERVICE_NAME, s_dstpath);
    OFCOM_MSG_PRINTF2(UISVR_MSG_DELIMITER, SERVICE_NAME, deli_form);

    /* convert delimiter */
    retval = _convert_delim();
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_INTERNAL_FUNC_ERROR, SERVICE_NAME, "_convert_delim", retval);
        sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "_convert_delim", retval);
        goto _DSLOAD_MAIN_ERR_TPFAIL_01;
    }

    /* uisvr login process */
    retval = uisvr_login_process(SERVICE_NAME, rcv_buf);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_INNER_FUNCTION_ERROR, SERVICE_NAME, "uisvr_login_process", retval);
        sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "uisvr_login_process", retval);
        goto _DSLOAD_MAIN_ERR_TPFAIL_01;
    }

    /* library initialization */
    retval = _init_libraries();
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_INTERNAL_FUNC_ERROR, SERVICE_NAME, "_init_libraries", retval);
        sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "_init_libraries", retval);
        goto _DSLOAD_MAIN_ERR_TPFAIL_02;
    }

	/* check if source exists */
	retval = _check_src_exist(ret_msg);
	if (retval < 0) {
		OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_INTERNAL_FUNC_ERROR, SERVICE_NAME, "_check_src_exist",  retval);
		goto _DSLOAD_MAIN_ERR_TPFAIL_03;
	}

    /* check if file exist */
    retval = _check_dst_exist(ret_msg);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_INTERNAL_FUNC_ERROR, SERVICE_NAME, "_check_dst_exist", retval);
    /*  sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "_check_dst_exist", retval); -- composed already */
        goto _DSLOAD_MAIN_ERR_TPFAIL_03;
    }

    /* load the dataset */
    retval = _dsload_dataset(ret_msg);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_INTERNAL_FUNC_ERROR, SERVICE_NAME, "_dsload_dataset", retval);
    /*  sprintf(ret_msg, "%s: %s() failed. rc(%d)\n", SERVICE_NAME, "_dsload_dataset", retval); -- composed already */
        goto _DSLOAD_MAIN_ERR_TPFAIL_03;
    }

    /* print message */
    OFCOM_MSG_PRINTF2(UISVR_MSG_DS_LOAD_OK, SERVICE_NAME, s_dsname);

    /* finalize libraries */
    _final_libraries();
    uisvr_logout_process();

    /* fbput ret_msg */
    sprintf(ret_msg, "%s: Dataset Is Loaded Successfully.\nPath: [%s]\n", SERVICE_NAME, s_dstpath);
    svrcom_fbput(snd_buf, FB_RETMSG, ret_msg, 0);

    /* service end */
    svrcom_svc_end(TPSUCCESS, 0);
    tpreturn(TPSUCCESS, 0, (char *)snd_buf, 0, TPNOFLAGS);

_DSLOAD_MAIN_ERR_TPFAIL_03:
    _final_libraries();

_DSLOAD_MAIN_ERR_TPFAIL_02:
    uisvr_logout_process();

_DSLOAD_MAIN_ERR_TPFAIL_01:
	if( retval == SAF_ERR_NOT_AUTHORIZED )
		sprintf(ret_msg, "You are not authorized to access this resource.\n");
			
    if ( snd_buf ) svrcom_fbput(snd_buf, FB_RETMSG, ret_msg, 0);

_DSLOAD_MAIN_ERR_TPFAIL_00:
    svrcom_svc_end(TPFAIL, retval);
    UISVR_TPRETURN_TPFAIL_CHECK_DISCONNECTED(retval, (char *)snd_buf, 0, TPNOFLAGS);
}


static int _get_params(FBUF *rcv_buf)
{
    int  retval;
    char load_type;
 
    /* fbget dataset name */
    retval = svrcom_fbget(rcv_buf, FB_DSNAME, s_dsname, DS_DSNAME_LEN);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_SVRCOM_FUNCTION_ERROR, SERVICE_NAME, "svrcom_fbget", retval);
        return retval;
    }

    /* fbget member name */
    if( dscom_is_relaxed_member_limit() )
        retval = svrcom_fbget(rcv_buf, FB_MEMNAME, s_member, DS_MEMBER_LEN2);
    else
        retval = svrcom_fbget(rcv_buf, FB_MEMNAME, s_member, DS_MEMBER_LEN);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_SVRCOM_FUNCTION_ERROR, SERVICE_NAME, "svrcom_fbget", retval);
        return retval;
    }

    /* fbget user catalog */
    retval = svrcom_fbget_opt(rcv_buf, FB_CATNAME, s_catalog, DS_DSNAME_LEN);
    if (retval < 0 && retval != SVRCOM_ERR_FBNOENT ) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_SVRCOM_FUNCTION_ERROR, SERVICE_NAME, "svrcom_fbget", retval);
        return retval;
    } else if (retval == SVRCOM_ERR_FBNOENT) {
        s_catalog[0] = '\0';
    }

    /* fbget dataset filepath */
    retval = svrcom_fbget(rcv_buf, FB_FILEPATH, s_dstpath, sizeof(s_dstpath) - 1);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_SVRCOM_FUNCTION_ERROR, SERVICE_NAME, "svrcom_fbget", retval);
        return retval;
    }
	
	
    /* fbget volume */
    retval = svrcom_fbget_opt(rcv_buf, FB_VOLUME, s_volser, DS_VOLSER_LEN);
    if (retval < 0 && retval != SVRCOM_ERR_FBNOENT) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_SVRCOM_FUNCTION_ERROR, SERVICE_NAME, "svrcom_fbget_opt", retval);
        return retval;
    }
	
	/* fbget deli_form from FB_ARGS */
    retval = svrcom_fbget_opt(rcv_buf, FB_ARGS, deli_form, sizeof(deli_form) - 1);
    if (retval < 0 && retval != SVRCOM_ERR_FBNOENT) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_SVRCOM_FUNCTION_ERROR, SERVICE_NAME, "svrcom_fbget_opt", retval);
        return retval;
    }
	

    /* initialize global variables */
    _file_check = 1;

    /* fbget multiple options */
    while (1) {
        if (svrcom_fbget(rcv_buf, FB_TYPE, &load_type, 1) < 0) break;
// delimeter 추가 처리 필요
        if (load_type == 'L' || load_type == 'l') {
            strcpy(deli_form, "\\n");
        } else if (load_type == 'F' || load_type == 'f') {
            _file_check = 0;
        }
    }

    return 0;
}

static int _validate_param(char *ret_msg)
{
	int retval;

	/* check if dataset name is specified */
	if (s_dsname[0] != '\0') {
		/* check if dataset name is invalid */
		retval = dscom_check_dsname(s_dsname);
		if (retval < 0) {
			OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_INVALID_DSN_ERROR, SERVICE_NAME, s_dsname);
			sprintf(ret_msg, "%s: invalid dataset name. dsname=%s\n", SERVICE_NAME, s_dsname);
	
			return retval;
		}
	}

	/* check if member name is specified */
	if (s_member[0] != '\0') {
		/* check if member name is invalid */
		retval = dscom_check_dsname2(s_dsname, s_member);
		if (retval < 0) {
			OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_INVALID_MEMBER_ERROR, SERVICE_NAME, s_member);
			sprintf(ret_msg, "%s: invalid member name. member=%s\n", SERVICE_NAME, s_member);

			return retval;
		}
	}

	/* check if user catalog is specified */
	if (s_catalog[0] != '\0') {
		/* check if catalog name is invalid */
		retval = dscom_check_dsname(s_catalog);
		if (retval < 0) {
			OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_INVALID_CATNAME_ERROR, SERVICE_NAME, s_catalog);
			sprintf(ret_msg, "%s: invalid catalog name. catalog=%s\n", SERVICE_NAME, s_catalog);

			return retval;
		}
	}
	
	/* check if volume serial is specified */
	if (s_volser[0] != '\0')
	{
		retval = dscom_check_volser(s_volser);
		if (retval < 0)
		{
			OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_INVALID_VOL_ERROR, SERVICE_NAME, s_volser);
			sprintf(ret_msg, "%s: invalid volume serial. volser=%s\n", SERVICE_NAME, s_volser);
			
			return retval;
		}
	}
	
	return 0;
}


static int _adjust_param()
{
    int  retval;
    char s_temp[1024];

    /* use temp directory if filepath is not specified */
    if (s_dstpath[0] == 0x00) {
        retval = ofcom_conf_get_value("dstool.conf", "DSLOAD", "LOAD_DIR", s_temp, sizeof(s_temp));
        if (retval < 0) {
            OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_OFCOM_FUNCTION_ERROR, SERVICE_NAME, "ofcom_conf_get_value", retval);
            return retval;
        }

        strcpy(s_dstpath, s_temp);
        strcat(s_dstpath, "/");
        strcat(s_dstpath, s_dsname);

        if (s_member[0] != 0x00) {
            strcat(s_dstpath, ".");
            strcat(s_dstpath, s_member);
        }
    }

    /* use config setting if delimiter is not specified */
    if (deli_form[0] == 0x00) {
        retval = ofcom_conf_get_value("dstool.conf", "DSLOAD", "DELIMITER", s_temp, sizeof(s_temp));
        if (retval < 0) {
            strcpy(deli_form, "");
        } else {
            if (!strcmp(s_temp, "NEWLINE")) strcpy(deli_form, "\\n");
            else strcpy(deli_form, s_temp);
        }
    }

    return 0;
}


static int _convert_delim()
{
    int  length, i;
    char ch, *ptr;

    /* initialize delimiter */
    memset(delimiter,0x00,sizeof(delimiter));
    ptr = delimiter;

    /* convert delimiter */
    if (deli_form[0]) {
        length = strlen(deli_form);
        for( i = 0; i < length; i++ ) {
            if( deli_form[i] == '\\' && i+1 < length ) {
                i++; ch = deli_form[i];
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
            } else *ptr++ = deli_form[i];
        }
    }

    return 0;
}


static int _init_libraries()
{
    int retval;

    /* dsio_batch_initialize */
    retval = dsio_batch_initialize(DSIO_BATCH_INIT_BOTH);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_DSIO_FUNCTION_ERROR, SERVICE_NAME, "dsio_batch_initialize", retval);
        goto _INIT_LIBRARIES_ERR_RETURN_00;
    }

    /* ams initialize */
    retval = ams_initialize();
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "ams_initialize", retval);
        goto _INIT_LIBRARIES_ERR_RETURN_01;
    }

    /* dsio_initialize */
    retval = dsalc_initialize();
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_DSALC_FUNCTION_ERROR, SERVICE_NAME, "dsalc_initialize", retval);
        goto _INIT_LIBRARIES_ERR_RETURN_02;
    }

    return 0;

_INIT_LIBRARIES_ERR_RETURN_02:
    ams_finalize();

_INIT_LIBRARIES_ERR_RETURN_01:
    dsio_batch_finalize();

_INIT_LIBRARIES_ERR_RETURN_00:
    return retval;
}


static int _final_libraries()
{
    /* dsalc finalization */
    dsalc_finalize();

    /* ams finalization */
    ams_finalize();

    /* dsio batch finalization */
    dsio_batch_finalize();

    return 0;
}


static int _check_src_exist(char *ret_msg)
{
	int retval, rcount = 1; icf_result_t result[1];
	ams_info_nvsm_t nvsm_info;
	char filepath[256]; struct stat filestat;
	
	int search_flags;
	char *cutpos;
	
	/* check if wild card is used */
	if (strchr(s_dsname, '*') || strchr(s_dsname, '%'))
	{
		OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_NO_WILD_CARD_ALLOWD_ERROR, SERVICE_NAME, s_dsname);
		sprintf(ret_msg, "%s: wild card character is not allowed in dsname - dsname=%s\n", SERVICE_NAME, s_dsname);
		return -1;
	}
	
	// dsname-member separation
	if ((cutpos = strchr(s_dsname, '(')))
	{
		*cutpos = '\0';
		strcpy(s_member, cutpos+1);
		if ((cutpos = strchr(s_member, ')')))
			*cutpos = '\0';
	}
	
	if (!s_volser[0])
	{
		/* call a function to set search catalog */
		retval = ams_use_catalog(s_catalog);
		if (retval < 0)
		{
			OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "ams_use_catalog", retval);
			sprintf(ret_msg, "%s: %s() failed. rc(%d)\n", SERVICE_NAME, "ams_use_catalog", retval);
			return retval;
		}
		
		/* set search options for ams_search_entries() */
		if (s_catalog[0])
			search_flags = AMS_SEARCH_1_CATALOG;
		else
			search_flags = AMS_SEARCH_DEFAULT;
		
		/* search source dataset in the catalog */
		retval = ams_search_entries(s_dsname, "AH", &rcount, result, search_flags);
		if (retval < 0 && retval != AMS_ERR_NOT_FOUND)
		{
			OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "ams_search_entries", retval);
			sprintf(ret_msg, "%s: %s() failed. rc(%d)\n", SERVICE_NAME, "ams_search_entries", retval);
			return retval;
		}
		
		// couldn't find dataset from the catalog
		if (retval == AMS_ERR_NOT_FOUND)
		{
			// get default volume
			retval = volm_get_default_volume(s_volser);
			if(retval < 0)
			{
				OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_VOLM_FUNCTION_ERROR, SERVICE_NAME, "volm_get_default_volume", retval);
				sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "volm_get_default_volume", retval);
				return -1;
			}
		}
		// dataset is in the catalog
		else
		{
			/* retrieve information from catalog */
			retval = ams_info(result[0].catname, result[0].entname, result[0].enttype, &nvsm_info, AMS_INFO_DEFAULT);
			if (retval < 0)
			{
				OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "ams_info", retval);
				sprintf(ret_msg, "%s: %s() failed. rc(%d)\n", SERVICE_NAME, "ams_info", retval);
				return retval;
			}
			
			/* check if user catalog is not specified */
			if (!s_catalog[0])
				strcpy(s_catalog, result[0].catname);
			
			/* check if member name is specified but dataset is not a PDS */
			if (s_member[0] && strncmp(nvsm_info.dsorg, "PO", 2))
			{
				OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_DATASET_IS_NOT_PDS_ERROR, SERVICE_NAME, s_dsname);
				sprintf(ret_msg, "dataset is not a PDS whlie member name is specified - dsname=%s\n", s_dsname);
				return -1;
			}
			
			// get volume serial information
			strcpy(s_volser, nvsm_info.volser);
		}
	}
	/* get volume path from volume serial */
	retval = volm_get_volume_path(s_volser, filepath);
	if(retval < 0)
	{
		OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_VOLM_FUNCTION_ERROR, SERVICE_NAME, "volm_get_volume_path", retval);
		sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "volm_get_volume_path", retval);
		return -1;
	}
		
	/* compose filepath */
	strcat(filepath, "/");
	strcat(filepath, s_dsname);
		
	/* check file exist */
	retval = lstat(filepath, & filestat);
	if( retval < 0 )
	{
		/*  OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "dataset_name", retval); */
		sprintf(ret_msg, "%s: dataset is not found in the volume - dsname=%s,volser=%s\n", SERVICE_NAME, s_dsname, s_volser);
		return -1;
	}
	
	return 0;
}


static int _check_dst_exist(char *ret_msg)
{
    int fd;

    /* check to see if file exist */
    if (_file_check) {
        fd = open(s_dstpath, O_RDONLY);
        if (fd >= 0) {
            OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_FILE_EXIST_ERROR, SERVICE_NAME, s_dstpath);
            sprintf(ret_msg, "%s: file %s already exist.\n", SERVICE_NAME, s_dstpath);
            close(fd); return SVRCOM_ERR_INVALID_PARAM;
        }
    }

    return 0;
}


static int _dsload_dataset(char *ret_msg)
{
    int retval, fdin, fdout, dcb_type, maxlen;
    char s_composed[256];

    int handle = 0;
    dsalc_req_t req;

    /* set user catalog */
    retval = ams_use_catalog(s_catalog);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "ams_use_catalog", retval);
        sprintf(ret_msg, "%s: %s() failed. rc(%d)\n", SERVICE_NAME, "ams_use_catalog", retval);
        goto _DSLOAD_DATASET_ERR_RETURN_00;
    }

    /* dsalc_allocate */
    memset(&req, 0x00, sizeof(req));
    req.disp.status = DISP_STATUS_SHR;
    req.disp.normal = DISP_TERM_KEEP;
    req.disp.abnormal = DISP_TERM_KEEP;

    /* dataset name */
    if (s_member[0]) {
        sprintf(s_composed, "%s(%s)", s_dsname, s_member);
    } else {
        strcpy(s_composed, s_dsname);
    }

    /* set lock wait flag */
    req.lock.lock_wait = LOCKM_LOCK_WAIT_IMMEDIATE;

    /* allocate dataset */
    handle = dsalc_allocate("DSLOAD", s_composed, &req, DSALC_ALLOCATE_DEFAULT);
    if (handle < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_DSALC_FUNCTION_ERROR, SERVICE_NAME, "dsalc_allocate", handle);
        sprintf(ret_msg, "%s: %s() failed. rc(%d)\n", SERVICE_NAME, "dsalc_allocate", handle);
        retval = handle; goto _DSLOAD_DATASET_ERR_RETURN_00;
    }

	/* get dcb_type */
	retval = dsio_dcb_get_type(dsalc_get_dcbs(handle), & dcb_type, NULL);
	if (retval < 0 ) {
		OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_DSIO_FUNCTION_ERROR, SERVICE_NAME, "dsio_dcb_get_type", retval);
		sprintf(ret_msg, "%s: %s() failed. rc(%d)\n", SERVICE_NAME, "dsio_dcb_get_type", retval);
		goto _DSLOAD_DATASET_ERR_RETURN_03;
	}

	/* check VSAM type */
	if (dcb_type != DSIO_DCB_TYPE_NVSM && dcb_type != DSIO_DCB_TYPE_ISAM && dcb_type != DSIO_DCB_TYPE_BDAM) {
		OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_TSAM_NOT_SUPPORT_ERROR, SERVICE_NAME, s_composed);
		sprintf(ret_msg, "%s: TSAM dataset is not supported. dsname=%s\n", SERVICE_NAME, s_composed);
		retval = -1; goto _DSLOAD_DATASET_ERR_RETURN_03;
	}

    /* get maximum length */
    retval = dsio_dcb_maxlrecl(dsalc_get_dcbs(handle), & maxlen, NULL);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_DSIO_FUNCTION_ERROR, SERVICE_NAME, "dsio_dcb_maxlrecl", retval);
        sprintf(ret_msg, "%s: %s() failed. rc(%d)\n", SERVICE_NAME, "dsio_dcb_maxlrecl", retval);
        goto _DSLOAD_DATASET_ERR_RETURN_03;
    }

    /* check_dataset_size */
    retval = _check_dataset_size(s_composed, dsalc_get_dcbs(handle), ret_msg);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_INTERNAL_FUNC_ERROR, SERVICE_NAME, "_check_dataset_size", retval);
    /*  sprintf(ret_msg, "%s: %s() failed. rc(%d)\n", SERVICE_NAME, "_check_dataset_size", retval); -- composed already */
        goto _DSLOAD_DATASET_ERR_RETURN_03;
    }

    /* dsio_batch_open */
    fdin = dsio_batch_open(dsalc_get_dcbs(handle), DSIO_OPEN_INPUT | DSIO_ACCESS_SEQUENTIAL | DSIO_LOCK_EXCLUSIVE);
    if (fdin < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_DSIO_FUNCTION_ERROR, SERVICE_NAME, "dsio_batch_open", fdin);
        sprintf(ret_msg, "%s: %s() failed. rc(%d)\n", SERVICE_NAME, "dsio_batch_open", fdin);
        retval = fdin; goto _DSLOAD_DATASET_ERR_RETURN_03;
    }
   
    /* open output file */
    fdout = open(s_dstpath, O_RDWR | O_CREAT | O_TRUNC, 0666);
    if (fdout < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_FILE_OPEN_ERROR, SERVICE_NAME, s_dstpath, strerror(errno));
        sprintf(ret_msg, "%s: file open error. path(%s),err(%s)\n", SERVICE_NAME, s_dstpath, strerror(errno));
        retval = fdout; goto _DSLOAD_DATASET_ERR_RETURN_04;
    }

    /* dsload_dataset_inner */
    retval = _dsload_dataset_inner(fdin, fdout, maxlen, delimiter);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_INTERNAL_FUNC_ERROR, SERVICE_NAME, "_dsload_dataset_inner", retval);
        sprintf(ret_msg, "%s: %s() failed. rc(%d)\n", SERVICE_NAME, "_dsload_dataset_inner", retval);
        goto _DSLOAD_DATASET_ERR_RETURN_05;
    }

    /* close output file */
    retval = close(fdout);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_FILE_CLOSE_ERROR, SERVICE_NAME, fdout, strerror(errno));
        sprintf(ret_msg, "%s: file close error. fd(%d),err(%s)\n", SERVICE_NAME, fdout, strerror(errno));
        goto _DSLOAD_DATASET_ERR_RETURN_04;
    }

    /* dsio_batch_close */
    retval = dsio_batch_close(fdin, DSIO_CLOSE_DEFAULT);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_DSIO_FUNCTION_ERROR, SERVICE_NAME, "dsio_batch_close", retval);
        sprintf(ret_msg, "%s: %s() failed. rc(%d)\n", SERVICE_NAME, "dsio_batch_close", retval);
        goto _DSLOAD_DATASET_ERR_RETURN_03;
    }

    /* dsalc_dispose */
    retval = dsalc_dispose(handle, DISP_COND_NORMAL, DSALC_UNALLOCATE_DEFAULT);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_DSALC_FUNCTION_ERROR, SERVICE_NAME, "dsalc_dispose", retval);
        sprintf(ret_msg, "%s: %s() failed. rc(%d)\n", SERVICE_NAME, "dsalc_dispose", retval);
        goto _DSLOAD_DATASET_ERR_RETURN_03;
    }

    /* dsalc_unallocate */
    retval = dsalc_unallocate(handle, DISP_COND_NORMAL, DSALC_UNALLOCATE_DEFAULT);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_DSALC_FUNCTION_ERROR, SERVICE_NAME, "dsalc_unallocate", retval);
        sprintf(ret_msg, "%s: %s() failed. rc(%d)\n", SERVICE_NAME, "dsalc_unallocate", retval);
        goto _DSLOAD_DATASET_ERR_RETURN_00;
    }

    return 0;

_DSLOAD_DATASET_ERR_RETURN_05:
    close(fdout);

_DSLOAD_DATASET_ERR_RETURN_04:
    dsio_batch_close(fdin, DSIO_CLOSE_DEFAULT);

_DSLOAD_DATASET_ERR_RETURN_03:
    dsalc_unallocate(handle, DISP_COND_ABNORMAL, DSALC_UNALLOCATE_DEFAULT);

_DSLOAD_DATASET_ERR_RETURN_00:
	if (fdout >= 0 )
		unlink(s_dstpath);

    return retval;
}


static int _check_dataset_size(char *dsname, dsio_dcb_t **dcbs, char *ret_msg)
{
    int retval; int64_t data_size;
    char s_temp[1024];

    /* read config value */
    retval = ofcom_conf_get_value("dstool.conf", "DSLOAD", "SIZE_LIMIT", s_temp, sizeof(s_temp));
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_OFCOM_FUNCTION_ERROR, SERVICE_NAME, "ofcom_conf_get_value", retval);
        sprintf(ret_msg, "%s: Get configuration failed. key=%s\n", SERVICE_NAME, "SIZE_LIMIT");
        return retval;
    }

    /* get dataset size */
    retval = dsio_dcb_report2(dcbs, 0, & data_size, NULL);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_DSIO_FUNCTION_ERROR, SERVICE_NAME, "dsio_dcb_report2", retval);
        sprintf(ret_msg, "%s: %s() failed. rc(%d)\n", SERVICE_NAME, "dsio_dcb_report2", retval);
        return retval;
    }

    /* check size limit */
    if (data_size > ofcom_strtoll(s_temp, NULL, 10)) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_DATASIZE_LIMIT_ERROR, SERVICE_NAME, dsname, data_size);
        sprintf(ret_msg, "%s: Dataset is greater than size limit. dsname=%s,size=%ld\n", SERVICE_NAME, dsname, data_size);
        return -1;
    }

    return 0;
}


static int _dsload_dataset_inner(int fdin, int fdout, int maxlen, char *delim)
{
    int retval, i, delim_size, buflen;
    char *buf = NULL;

    delim_size = strlen(delim);

    buf = malloc(maxlen + delim_size + 1);
    if( ! buf ) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_MALLOC_ERROR, SERVICE_NAME, "buf", strerror(errno));
        return -1;
    }

    while( 1 ) {
        buflen = maxlen;

        retval = dsio_batch_read2(fdin, 0, NULL, 0, buf, & buflen, DSIO_FLAG_NEXT);
        if( retval < 0 && retval != DSIO_ERR_END_OF_FILE ) {
            OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_DSIO_FUNCTION_ERROR, SERVICE_NAME, "dsio_batch_read2", retval);
            free(buf); return retval;
        }

        if( retval == DSIO_ERR_END_OF_FILE ) break;

        if( delim_size > 0 ) {
            for( i = 0; i < delim_size; i++ ) buf[buflen+i] = delim[i];
            buflen = buflen + delim_size;
        }

        retval = write(fdout, buf, buflen);
        if( retval < 0 ) {
            OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_FILE_WRITE_ERROR, SERVICE_NAME, fdout, strerror(errno));
            free(buf); return retval;
        }
    }

    free(buf);
    return 0;
}
