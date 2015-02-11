#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include <usrinc/atmi.h>
#include <usrinc/fbuf.h>

#include "uisvr.h"
#include "ofcom.h"
#include "svrcom.h"
#include "ds.h"
#include "dsio_batch.h"
#include "ams.h"
#include "dsalc.h"

#include "oframe_fdl.h"
#include "msgcode_uisvr.h"

/**
 * Service Name:
 *      OFRUISVRDSTOUCH
 *
 * Description:
 *      Update dataset status
 *
 * Input:
 *      FB_TACF_TOKEN(string): TACF token
 *      FB_DSNAME(string): dataset name
 *      FB_MEMNAME(string): member name
 *      FB_VOLUME(string): volume serial
 *      FB_CATNAME(string): catalog name
 *      FB_TIMESTAMP(string): creation date
 *
 * Output:
 *      FB_RETMSG(string): error message
 */

#define SERVICE_NAME    "OFRUISVRDSTOUCH"


static char dataset_name[256], member_name[256], volume_serial[256], user_catalog[256];
static char creation_date[256];

extern int dsalc_new_allocate_method;

static int _get_params(FBUF *rcv_buf);
static int _validate_param(char *ret_msg);

static int _init_libraries();
static int _final_libraries();

static int _check_ds_exist(char *ret_msg);
static int _touch_dataset(char *ret_msg);


void OFRUISVRDSTOUCH(TPSVCINFO *tpsvcinfo)
{
    int retval;

    FBUF *rcv_buf = NULL;
    FBUF *snd_buf = NULL;

    char s_temp[1024];
    char ret_msg[1024] = {'\0',};

    /* service start */
    retval = svrcom_svc_start(SERVICE_NAME);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_SVRCOM_FUNCTION_ERROR, SERVICE_NAME, "svrcom_svc_start", retval);
        goto _DSTOUCH_MAIN_ERR_TPFAIL_00;
    }

    /* get rcv buffer */
    rcv_buf = (FBUF *)(tpsvcinfo->data);
    fbprint(rcv_buf);
    
    /* allocate snd buffer */
    snd_buf = (FBUF *)tpalloc("FIELD", NULL, 0);
    if (!snd_buf) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_TMAX_TPALLOC_ERROR, SERVICE_NAME, "snd_buf", tpstrerror(tperrno));
        retval = SVRCOM_ERR_TPALLOC; goto _DSTOUCH_MAIN_ERR_TPFAIL_00;
    }

    /* initialize parameter */
    memset(dataset_name,0x00,sizeof(dataset_name));
    memset(member_name,0x00,sizeof(member_name));
    memset(volume_serial,0x00,sizeof(volume_serial));
    memset(user_catalog,0x00,sizeof(user_catalog));
    memset(creation_date,0x00,sizeof(creation_date));

    /* get parameters */    
    retval = _get_params(rcv_buf);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_INTERNAL_FUNC_ERROR, SERVICE_NAME, "_get_params", retval);
        sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "_get_params", retval);
        goto _DSTOUCH_MAIN_ERR_TPFAIL_01;
    }
    
	/* validate parameters */
    retval = _validate_param(ret_msg);
    if (retval < 0) 
        goto _DSTOUCH_MAIN_ERR_TPFAIL_01;

    /* uisvr login process */
    retval = uisvr_login_process(SERVICE_NAME, rcv_buf);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_INNER_FUNCTION_ERROR, SERVICE_NAME, "uisvr_login_process", retval);
        sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "uisvr_login_process", retval);
        goto _DSTOUCH_MAIN_ERR_TPFAIL_01;
    }

    /* library initialization */
    retval = _init_libraries();
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_INTERNAL_FUNC_ERROR, SERVICE_NAME, "_init_libraries", retval);
        sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "_init_libraries", retval);
        goto _DSTOUCH_MAIN_ERR_TPFAIL_02;
    }

    /* check ds existance */
    retval = _check_ds_exist(s_temp);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_INTERNAL_FUNC_ERROR, SERVICE_NAME, "_check_ds_exist", retval);
        sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "_check_ds_exist", retval); strcat(ret_msg, s_temp); 
        goto _DSTOUCH_MAIN_ERR_TPFAIL_03;
    }

    /* touch the dataset */
    retval = _touch_dataset(ret_msg);
    if (retval < 0) 
        goto _DSTOUCH_MAIN_ERR_TPFAIL_03;

    /* finalize libraries */
    _final_libraries();
    uisvr_logout_process();

    /* fbput ret_msg */
    sprintf(ret_msg, "%s: Dataset Is Touched Successfully. dsn=%s\n", SERVICE_NAME, dataset_name);
    svrcom_fbput(snd_buf, FB_RETMSG, ret_msg, 0);

    /* service end */
    svrcom_svc_end(TPSUCCESS, 0);
    tpreturn(TPSUCCESS, 0, (char *)snd_buf, 0, TPNOFLAGS);

_DSTOUCH_MAIN_ERR_TPFAIL_03:
    _final_libraries();

_DSTOUCH_MAIN_ERR_TPFAIL_02:
    uisvr_logout_process();

_DSTOUCH_MAIN_ERR_TPFAIL_01:
	if( retval == SAF_ERR_NOT_AUTHORIZED )
		sprintf(ret_msg, "You are not authorized to access this resource.\n");
			
    if (snd_buf) svrcom_fbput(snd_buf, FB_RETMSG, ret_msg, 0);

_DSTOUCH_MAIN_ERR_TPFAIL_00:
    svrcom_svc_end(TPFAIL, retval);
    UISVR_TPRETURN_TPFAIL_CHECK_DISCONNECTED(retval, (char *)snd_buf, 0, TPNOFLAGS);
}


static int _get_params(FBUF *rcv_buf)
{
    int retval;

    /* get dsname */
    retval = svrcom_fbget(rcv_buf, FB_DSNAME, dataset_name, DS_DSNAME_LEN);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_SVRCOM_FUNCTION_ERROR, SERVICE_NAME, "svrcom_fbget", retval);
        return retval;
    }

    /* get member */
    if( dscom_is_relaxed_member_limit() )
        retval = svrcom_fbget_opt(rcv_buf, FB_MEMNAME, member_name, DS_MEMBER_LEN2);
    else
        retval = svrcom_fbget_opt(rcv_buf, FB_MEMNAME, member_name, DS_MEMBER_LEN);
    if (retval < 0 && retval != SVRCOM_ERR_FBNOENT) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_SVRCOM_FUNCTION_ERROR, SERVICE_NAME, "svrcom_fbget", retval);
        return retval;
    }

    /* get volume serial */
    retval = svrcom_fbget_opt(rcv_buf, FB_VOLUME, volume_serial, DS_VOLSER_LEN);
    if (retval < 0 && retval != SVRCOM_ERR_FBNOENT) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_SVRCOM_FUNCTION_ERROR, SERVICE_NAME, "svrcom_fbget", retval);
        return retval;
    }

    /* get user catalog */
    retval = svrcom_fbget_opt(rcv_buf, FB_CATNAME, user_catalog, DS_DSNAME_LEN);
    if (retval < 0 && retval != SVRCOM_ERR_FBNOENT) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_SVRCOM_FUNCTION_ERROR, SERVICE_NAME, "svrcom_fbget", retval);
        return retval;
    }

    /* get creation_date */
    retval = svrcom_fbget_opt(rcv_buf, FB_TIMESTAMP, creation_date, 8);
    if (retval < 0 && retval != SVRCOM_ERR_FBNOENT) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_SVRCOM_FUNCTION_ERROR, SERVICE_NAME, "svrcom_fbget", retval);
        return retval;
    }

    return 0;
}


static int _validate_param(char *ret_msg)
{
    /* check if dataset name is specified */
    if( dataset_name[0] != '\0' ) {
        /* check if dataset name is invalid */
        if( dscom_check_dsname(dataset_name) < 0 ) {
			OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_INVALID_DSN_ERROR, SERVICE_NAME, dataset_name);
			sprintf(ret_msg, "%s: invalid dataset name. dsname=%s\n", SERVICE_NAME, dataset_name);
            return SVRCOM_ERR_INVALID_PARAM;
        }
    }

    /* check if member name is specified */
    if( member_name[0] != '\0' ) {
        /* check if member name is invalid */
        if( dscom_check_dsname2(dataset_name, member_name) < 0 ) {
			OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_INVALID_MEMBER_ERROR, SERVICE_NAME, member_name);
			sprintf(ret_msg, "%s: invalid member name. member=%s\n", SERVICE_NAME, member_name);
            return SVRCOM_ERR_INVALID_PARAM;
        }
    }

    /* check if user catalog is specified */
    if( user_catalog[0] != '\0' ) {
        /* check if user catalog is invalid */
        if( dscom_check_dsname(user_catalog) < 0 ) {
			OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_INVALID_CATNAME_ERROR, SERVICE_NAME, user_catalog);
			sprintf(ret_msg, "%s: invalid catalog name. catalog=%s\n", SERVICE_NAME, user_catalog);
            return SVRCOM_ERR_INVALID_PARAM;
        }
    }

    /* check if volume serial is specified */
    if( volume_serial[0] != '\0' ) {
        /* check if volume serial is invalid */
        if( dscom_check_volser(volume_serial) < 0 ) {
			OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_INVALID_VOL_ERROR, SERVICE_NAME, volume_serial);
			sprintf(ret_msg, "%s: invalid volume serial. volume=%s\n", SERVICE_NAME, volume_serial);
            return SVRCOM_ERR_INVALID_PARAM;
        }
    }

    /* check if creation date is specified */
    if( creation_date[0] != '\0' ) {
        /* decompose date into year, month, day */
		if( dscom_check_credt(creation_date) < 0 ) {
			OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_INVALID_CREAT_DATE_ERROR, SERVICE_NAME, creation_date);
			sprintf(ret_msg, "%s: invalid creation date. date=%s\n", SERVICE_NAME, creation_date);
			return SVRCOM_ERR_INVALID_PARAM;
		}
    } else {
    	/* set default date(sysdate) */
    	OFCOM_DATE_TO_STRING( creation_date, ofcom_sys_date() );
    }

    return 0;
}


static int _init_libraries()
{
    int retval;

    /* initialize dsio_batch library */
    retval = dsio_batch_initialize(DSIO_BATCH_INIT_BOTH);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_DSIO_FUNCTION_ERROR, SERVICE_NAME, "dsio_batch_initialize", retval);
        goto _INIT_LIBRARIES_ERR_RETURN_00;
    }

    /* initialize ams library */
    retval = ams_initialize();
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "ams_initialize", retval);
        goto _INIT_LIBRARIES_ERR_RETURN_01;
    }

    /* initialize dsalc library */
    retval = dsalc_initialize();
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_DSALC_FUNCTION_ERROR, SERVICE_NAME, "dsalc_initiailze", retval);
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

    /* dsio_batch finalization */
    dsio_batch_finalize();

    return 0;
}


static int _check_ds_exist(char *ret_msg)
{
    int retval;

    int search_flags;
    char *cutpos;

    int rcount = 1;
    icf_result_t result[1];

    char filepath[256];
    struct stat filestat;

    ams_info_nvsm_t nvsm_info;


    /* call a function to set search catalog */
    retval = ams_use_catalog(user_catalog);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "ams_use_catalog", retval);
        sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "ams_use_catalog", retval);
        return -1;
    }

    /* set sarch options for ams_search_entries() */
    if( user_catalog[0] ) search_flags = AMS_SEARCH_1_CATALOG;
    else search_flags = AMS_SEARCH_DEFAULT;

    /* check if wild card is used */
    if( strchr(dataset_name, '*') || strchr(dataset_name, '%') ) {
    /*  OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "dataset_name", retval); */
        sprintf(ret_msg, "%s: wild card character is not allowed in dsname - dsname=%s\n", SERVICE_NAME, dataset_name);
        return -1;
    }

    /* separate member name from dsname */
    if( (cutpos = strchr(dataset_name, '(') ) ) {
        *cutpos = '\0'; strcpy(member_name, cutpos+1);
        if( (cutpos = strchr(member_name, ')') ) ) *cutpos = '\0';
    }

    if( dsalc_new_allocate_method && volume_serial[0] ) {
        retval = AMS_ERR_NOT_FOUND;
    } else {
        /* search source dataset in the catalog */
        retval = ams_search_entries(dataset_name, "ACGHU", & rcount, result, search_flags);
        if( retval < 0 && retval != AMS_ERR_NOT_FOUND ) {
            OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "ams_search_entreis", retval);
            sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "ams_search_entries", retval);
            return -1;
        }
    }

    /* check if dataset is not found */
    if( retval == AMS_ERR_NOT_FOUND ) {
        /* check if catalog name is specified */
        if( user_catalog[0] ) {
            /*  OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "user_catalog", retval); */
            if( dsalc_new_allocate_method && volume_serial[0] ) {
                sprintf(ret_msg, "%s: catalog option(%s) cannot use with volume option(%s) on the DS_ALLOCATE_METHOD=NEW\n", SERVICE_NAME, user_catalog, volume_serial);
                return -1;
            } else {
                sprintf(ret_msg, "%s: dataset is not found in the catalog - dsname=%s,catalog=%s\n", SERVICE_NAME, dataset_name, user_catalog);
                return -1;
            }
        }

        /* check if volume serial is not specified */
        if( ! volume_serial[0] ) {
            retval = volm_get_default_volume(volume_serial);
            if( retval < 0 ) {
                OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_VOLM_FUNCTION_ERROR, SERVICE_NAME, "volm_get_default_volume", retval);
                sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "volm_get_default_volume", retval);
                return -1;
            }
        }

        /* get volume path from volume serial */
        retval = volm_get_volume_path(volume_serial, filepath);
        if( retval < 0 ) {
            OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_VOLM_FUNCTION_ERROR, SERVICE_NAME, "volm_get_volume_path", retval);
            sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "volm_get_volume_path", retval);
            return -1;
        }

        /* compose filepath */
        strcat(filepath, "/");
        strcat(filepath, dataset_name);

        /* check file exist */
        retval = lstat(filepath, & filestat);
        if( retval < 0 ) {
        /*  OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "dataset_name", retval); */
            sprintf(ret_msg, "%s: dataset is not found in the volume - dsname=%s,volser=%s\n", SERVICE_NAME, dataset_name, volume_serial);
            return -1;
        }

        /* check if member name is specified but dataset is not a PDS */
        if( member_name[0] && ! S_ISDIR(filestat.st_mode) ) {
        /*  OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "member_name", retval); */
            sprintf(ret_msg, "%s: dataset is not a PDS while member name is specified - dsname=%s\n", SERVICE_NAME, dataset_name);
            return -1;
        }

    /* check if dataset is Non-VSAM */
    } else if( AMS_IS_ENTRY_NONVSAM(result[0].enttype) ) {
        /* retrieve information from catalog */
        retval = ams_info(result[0].catname, result[0].entname, result[0].enttype, & nvsm_info, AMS_INFO_DEFAULT);
        if( retval < 0 ) {
            OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "ams_info", retval);
            sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "ams_info", retval);
            return -1;
        }

        /* check if volume serial is not specified */
        if( ! user_catalog[0] ) strcpy(user_catalog, result[0].catname);
        /* if( ! volume_serial[0] ) strcpy(volume_serial, nvsm_info.volser); */

        /* check if catalog information is different */
        if( volume_serial[0] && strcmp(volume_serial, nvsm_info.volser) ) {
        /*  OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "volume_serial", retval); */
            sprintf(ret_msg, "%s: volume serial information is different from the catalog - dsname=%s\n", SERVICE_NAME, dataset_name);
            return -1;
        }

        /* check if member name is specified but dataset is not a PDS */
        if( member_name[0] && strncmp(nvsm_info.dsorg, "PO", 2) ) {
        /*  OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "member_name", retval); */
            sprintf(ret_msg, "%s: dataset is not a PDS while member name is specified - dsname=%s\n", SERVICE_NAME, dataset_name);
            return -1;
        }

    /* otherwise dataset is VSAM */
    } else {
        /* check if user catalog is not specified */
        if( ! user_catalog[0] ) strcpy(user_catalog, result[0].catname);
    }

    return 0;
}


static int _touch_dataset(char *ret_msg)
{
    int retval;

    char dsname_conv[256];
    dsalc_req_t req;
    int handle;

    dsio_dcb_t **dcbs = NULL;
    int dcb_type;

    int rcount = 1;
    icf_result_t result;

    ams_info_nvsm_t nvsm_info;

    /* set user catalog */
    retval = ams_use_catalog(user_catalog);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "ams_use_catalog", retval);
        sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "ams_use_catalog", retval);
        goto _TOUCH_DATASET_ERR_RETURN_00;
    }

    /* prepare allocation dsname */
    if (member_name[0] != 0) sprintf(dsname_conv, "%s(%s)", dataset_name, member_name);
    else strcpy(dsname_conv, dataset_name);

    /* prepare allocation request */
    memset(&req, 0x00, sizeof(req));
    req.disp.status = DISP_STATUS_SHR;
    req.disp.normal = DISP_TERM_KEEP;
    req.disp.abnormal = DISP_TERM_KEEP;

    /* prepare request - volume */
    strcpy(req.volume.unit, "");
    strcpy(req.volume.vlist, volume_serial);

    /* prepare request - dslock */
    req.lock.lock_wait = LOCKM_LOCK_WAIT_IMMEDIATE; /* lock immediately */

    /* allocate dataset */
    handle = dsalc_allocate("DSTOUCH", dsname_conv, &req, DSALC_ALLOCATE_DEFAULT);
    if (handle < 0) {
        retval = handle; 
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_DSALC_FUNCTION_ERROR, SERVICE_NAME, "dsalc_allocate", retval);
        sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "dsalc_allocate", retval);
        goto _TOUCH_DATASET_ERR_RETURN_00;
    }

	/* change dataset creation date */
	if (creation_date[0])
	{
		if( dsalc_new_allocate_method && volume_serial[0] )
		{
			retval = AMS_ERR_NOT_FOUND;
		}
		else
		{
			/* search catalog entries */
			retval = ams_search_entries(dataset_name, "ACGHU", &rcount, &result, AMS_SEARCH_DEFAULT);
			if (retval < 0 && retval != AMS_ERR_NOT_FOUND)
			{
				OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "ams_search_entreis", retval);
				sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "ams_search_entries", retval);
				goto _TOUCH_DATASET_ERR_RETURN_04;
			}
		}
		
		/* check if dataset is not cataloged */
		if (retval == AMS_ERR_NOT_FOUND)
		{
			/* call ams function to update dataset creation date */
			retval = ams_touch_nvsm_ds(dataset_name, volume_serial, creation_date, AMS_TOUCH_DEFAULT);
			if (retval < 0)
			{
				OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "ams_touch_nvsm_ds", retval);
				sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "ams_touch_nvsm_ds", retval);
				goto _TOUCH_DATASET_ERR_RETURN_04;
			}
		}
		/* otherwise dataset is cataloged */
		else
		{
			/* call ams function to update dataset creation date */
			retval = ams_touch(result.catname, result.entname, result.enttype, creation_date, AMS_TOUCH_DEFAULT);
			if (retval < 0)
			{
				OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "ams_touch", retval);
				sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "ams_touch", retval);
				goto _TOUCH_DATASET_ERR_RETURN_04;
			}
			 /* ams_touch() doesn't support touching pds member */
			if (member_name[0])
			{/* retrieve information from catalog */
				retval = ams_info(result.catname, result.entname, result.enttype, & nvsm_info, AMS_INFO_DEFAULT);
				 if( retval < 0 )
				 {
					OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "ams_info", retval);
					sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "ams_info", retval);
					goto _TOUCH_DATASET_ERR_RETURN_04;
				 }
				 /* call ams function to update pds member creation date */
				 retval = ams_touch_nvsm_ds(dsname_conv, nvsm_info.volser, creation_date, AMS_TOUCH_DEFAULT);
				if (retval < 0)
				{
					OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "ams_touch_nvsm_ds", retval);
					sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "ams_touch_nvsm_ds", retval);
					goto _TOUCH_DATASET_ERR_RETURN_04;
				}
			}
		}
	}
	
    /* retrieve dcbs from allocation handle */
    dcbs = dsalc_get_dcbs(handle);
    if (! dcbs) {
        retval = -1;
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_DSALC_FUNCTION_ERROR, SERVICE_NAME, "dsalc_get_dcbs", retval);
        sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "dsalc_get_dcbs", retval);
        goto _TOUCH_DATASET_ERR_RETURN_04;
    }

    /* retrieve dcb_type & dcb_count from dcbs */
    retval = dsio_dcb_get_type(dcbs, & dcb_type, NULL);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_DSIO_FUNCTION_ERROR, SERVICE_NAME, "dsio_dcb_get_type", retval);
        sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "dsio_dcb_get_type", retval);
        goto _TOUCH_DATASET_ERR_RETURN_04;
    }

    /* retrieve volume serial from dcbs structure */
    retval = dsio_dcb_volser(dcbs, 0, volume_serial);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_DSIO_FUNCTION_ERROR, SERVICE_NAME, "dsio_dcb_volser", retval);
        sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "dsio_dcb_volser", retval);
        goto _TOUCH_DATASET_ERR_RETURN_04;
    }

    /* check if dataset is non-vsam */
    if( dcb_type == DSIO_DCB_TYPE_NVSM ) {
        /* call a function to update DCB report */
        retval = dsio_update_report(dcbs, 0);
        if (retval < 0) {
            OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_DSIO_FUNCTION_ERROR, SERVICE_NAME, "dsio_update_report", retval);
            sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "dsio_update_report", retval);
            goto _TOUCH_DATASET_ERR_RETURN_04;
        }
    }

    /* dispose the dataset */
    retval = dsalc_dispose(handle, DISP_COND_NORMAL, DSALC_DISPOSE_DEFAULT);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_DSALC_FUNCTION_ERROR, SERVICE_NAME, "dsalc_dispose", retval);
        sprintf(ret_msg,"%s: %s() failed. rc=%d\n", SERVICE_NAME, "dsalc_dispose", retval);
        goto _TOUCH_DATASET_ERR_RETURN_04;
    }

    /* unallocate the dataset */
    retval = dsalc_unallocate(handle, DISP_COND_NORMAL, DSALC_UNALLOCATE_DEFAULT);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_DSALC_FUNCTION_ERROR, SERVICE_NAME, "dsalc_unallocate", retval);
        sprintf(ret_msg,"%s: %s() failed. rc=%d\n", SERVICE_NAME, "dsalc_unallocate", retval);
        goto _TOUCH_DATASET_ERR_RETURN_00;
    }

    return 0;

_TOUCH_DATASET_ERR_RETURN_04:
    dsalc_unallocate(handle, DISP_COND_ABNORMAL, DSALC_UNALLOCATE_DEFAULT);

_TOUCH_DATASET_ERR_RETURN_00:
    return retval;
}
