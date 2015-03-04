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
 *      FB_TYPE(char): touch option for using system section
 *
 * Output:
 *      FB_RETMSG(string): error message
 */

#define SERVICE_NAME    "OFRUISVRDSTOUCH"


static char dstouch_dsname[DS_DSNAME_LEN + 2];
static char dstouch_member[NVSM_MEMBER_LEN + 2];
static char dstouch_volser[DS_VOLSER_LEN + 2];
static char dstouch_catalog[DS_DSNAME_LEN + 2];
static char dstouch_credt[256];
static int dstouch_use_syssec;

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
    memset(dstouch_dsname,0x00,sizeof(dstouch_dsname));
    memset(dstouch_member,0x00,sizeof(dstouch_member));
    memset(dstouch_volser,0x00,sizeof(dstouch_volser));
    memset(dstouch_catalog,0x00,sizeof(dstouch_catalog));
    memset(dstouch_credt,0x00,sizeof(dstouch_credt));

    /* initialize parameter2 */
    dstouch_use_syssec = 0;

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
    sprintf(ret_msg, "%s: Dataset Is Touched Successfully. dsn=%s\n", SERVICE_NAME, dstouch_dsname);
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
    retval = svrcom_fbget(rcv_buf, FB_DSNAME, dstouch_dsname, DS_DSNAME_LEN);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_SVRCOM_FUNCTION_ERROR, SERVICE_NAME, "svrcom_fbget", retval);
        return retval;
    }

    /* get member */
    if( dscom_is_relaxed_member_limit() )
        retval = svrcom_fbget_opt(rcv_buf, FB_MEMNAME, dstouch_member, DS_MEMBER_LEN2);
    else
        retval = svrcom_fbget_opt(rcv_buf, FB_MEMNAME, dstouch_member, DS_MEMBER_LEN);
    if (retval < 0 && retval != SVRCOM_ERR_FBNOENT) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_SVRCOM_FUNCTION_ERROR, SERVICE_NAME, "svrcom_fbget", retval);
        return retval;
    }

    /* get volume serial */
    retval = svrcom_fbget_opt(rcv_buf, FB_VOLUME, dstouch_volser, DS_VOLSER_LEN);
    if (retval < 0 && retval != SVRCOM_ERR_FBNOENT) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_SVRCOM_FUNCTION_ERROR, SERVICE_NAME, "svrcom_fbget", retval);
        return retval;
    }

    /* get user catalog */
    retval = svrcom_fbget_opt(rcv_buf, FB_CATNAME, dstouch_catalog, DS_DSNAME_LEN);
    if (retval < 0 && retval != SVRCOM_ERR_FBNOENT) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_SVRCOM_FUNCTION_ERROR, SERVICE_NAME, "svrcom_fbget", retval);
        return retval;
    }

    /* get dstouch_credt */
    retval = svrcom_fbget_opt(rcv_buf, FB_TIMESTAMP, dstouch_credt, 8);
    if (retval < 0 && retval != SVRCOM_ERR_FBNOENT) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_SVRCOM_FUNCTION_ERROR, SERVICE_NAME, "svrcom_fbget", retval);
        return retval;
    }
    
    /* get dstouch_use_syssec */
    retval = svrcom_fbget_opt(rcv_buf, FB_TYPE, dstouch_credt, 8);
    if (retval < 0 && retval != SVRCOM_ERR_FBNOENT) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_SVRCOM_FUNCTION_ERROR, SERVICE_NAME, "svrcom_fbget", retval);
        return retval;
    }

    return 0;
}


static int _validate_param(char *ret_msg)
{
    /* check if dataset name is specified */
    if( dstouch_dsname[0] != '\0' ) {
        /* check if dataset name is invalid */
        if( dscom_check_dsname(dstouch_dsname) < 0 ) {
            OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_INVALID_DSN_ERROR, SERVICE_NAME, dstouch_dsname);
            sprintf(ret_msg, "%s: invalid dataset name. dsname=%s\n", SERVICE_NAME, dstouch_dsname);
            return SVRCOM_ERR_INVALID_PARAM;
        }
    }

    /* check if member name is specified */
    if( dstouch_member[0] != '\0' ) {
        /* check if member name is invalid */
        if( dscom_check_dsname2(dstouch_dsname, dstouch_member) < 0 ) {
            OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_INVALID_MEMBER_ERROR, SERVICE_NAME, dstouch_member);
            sprintf(ret_msg, "%s: invalid member name. member=%s\n", SERVICE_NAME, dstouch_member);
            return SVRCOM_ERR_INVALID_PARAM;
        }
    }

    /* check if user catalog is specified */
    if( dstouch_catalog[0] != '\0' ) {
        /* check if user catalog is invalid */
        if( dscom_check_dsname(dstouch_catalog) < 0 ) {
            OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_INVALID_CATNAME_ERROR, SERVICE_NAME, dstouch_catalog);
            sprintf(ret_msg, "%s: invalid catalog name. catalog=%s\n", SERVICE_NAME, dstouch_catalog);
            return SVRCOM_ERR_INVALID_PARAM;
        }
    }

    /* check if volume serial is specified */
    if( dstouch_volser[0] != '\0' ) {
        /* check if volume serial is invalid */
        if( dscom_check_volser(dstouch_volser) < 0 ) {
            OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_INVALID_VOL_ERROR, SERVICE_NAME, dstouch_volser);
            sprintf(ret_msg, "%s: invalid volume serial. volume=%s\n", SERVICE_NAME, dstouch_volser);
            return SVRCOM_ERR_INVALID_PARAM;
        }
    }

    /* check if creation date is specified */
    if( dstouch_credt[0] != '\0' ) {
        /* decompose date into year, month, day */
        if( dscom_check_credt(dstouch_credt) < 0 ) {
            OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_INVALID_CREAT_DATE_ERROR, SERVICE_NAME, dstouch_credt);
            sprintf(ret_msg, "%s: invalid creation date. date=%s\n", SERVICE_NAME, dstouch_credt);
            return SVRCOM_ERR_INVALID_PARAM;
        }
    } else {
        /* set default date(sysdate) */
        OFCOM_DATE_TO_STRING( dstouch_credt, ofcom_sys_date() );
    }

    return 0;
}


static int _init_libraries()
{
    int retval;
    
    /* dsio_batch_initialize(): consider -s option */
    if (dstouch_use_syssec) dsio_batch_use_sys1_config = 1;

    /* initialize dsio_batch library */
    retval = dsio_batch_initialize(DSIO_BATCH_INIT_BOTH);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_DSIO_FUNCTION_ERROR, SERVICE_NAME, "dsio_batch_initialize", retval);
        goto _INIT_LIBRARIES_ERR_RETURN_00;
    }

    /* ams_initialize(): consider -s option */
    if (dstouch_use_syssec) ams_use_sys1_config = 1;
    
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
    retval = ams_use_catalog(dstouch_catalog);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "ams_use_catalog", retval);
        sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "ams_use_catalog", retval);
        return -1;
    }

    /* set sarch options for ams_search_entries() */
    if( dstouch_catalog[0] ) search_flags = AMS_SEARCH_1_CATALOG;
    else search_flags = AMS_SEARCH_DEFAULT;

    /* check if wild card is used */
    if( strchr(dstouch_dsname, '*') || strchr(dstouch_dsname, '%') ) {
    /*  OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "dstouch_dsname", retval); */
        sprintf(ret_msg, "%s: wild card character is not allowed in dsname - dsname=%s\n", SERVICE_NAME, dstouch_dsname);
        return -1;
    }

    /* separate member name from dsname */
    if( (cutpos = strchr(dstouch_dsname, '(') ) ) {
        *cutpos = '\0'; strcpy(dstouch_member, cutpos+1);
        if( (cutpos = strchr(dstouch_member, ')') ) ) *cutpos = '\0';
    }

    if( dsalc_new_allocate_method && dstouch_volser[0] ) {
        retval = AMS_ERR_NOT_FOUND;
    } else {
        /* search source dataset in the catalog */
        retval = ams_search_entries(dstouch_dsname, "ACGHU", & rcount, result, search_flags);
        if( retval < 0 && retval != AMS_ERR_NOT_FOUND ) {
            OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "ams_search_entreis", retval);
            sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "ams_search_entries", retval);
            return -1;
        }
    }

    /* check if dataset is not found */
    if( retval == AMS_ERR_NOT_FOUND ) {
        /* check if catalog name is specified */
        if( dstouch_catalog[0] ) {
            /*  OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "dstouch_catalog", retval); */
            if( dsalc_new_allocate_method && dstouch_volser[0] ) {
                sprintf(ret_msg, "%s: catalog option(%s) cannot use with volume option(%s) on the DS_ALLOCATE_METHOD=NEW\n", SERVICE_NAME, dstouch_catalog, dstouch_volser);
                return -1;
            } else {
                sprintf(ret_msg, "%s: dataset is not found in the catalog - dsname=%s,catalog=%s\n", SERVICE_NAME, dstouch_dsname, dstouch_catalog);
                return -1;
            }
        }

        /* check if volume serial is not specified */
        if( ! dstouch_volser[0] ) {
            retval = volm_get_default_volume(dstouch_volser);
            if( retval < 0 ) {
                OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_VOLM_FUNCTION_ERROR, SERVICE_NAME, "volm_get_default_volume", retval);
                sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "volm_get_default_volume", retval);
                return -1;
            }
        }

        /* get volume path from volume serial */
        retval = volm_get_volume_path(dstouch_volser, filepath);
        if( retval < 0 ) {
            OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_VOLM_FUNCTION_ERROR, SERVICE_NAME, "volm_get_volume_path", retval);
            sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "volm_get_volume_path", retval);
            return -1;
        }

        /* compose filepath */
        strcat(filepath, "/");
        strcat(filepath, dstouch_dsname);

        /* check file exist */
        retval = lstat(filepath, & filestat);
        if( retval < 0 ) {
        /*  OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "dstouch_dsname", retval); */
            sprintf(ret_msg, "%s: dataset is not found in the volume - dsname=%s,volser=%s\n", SERVICE_NAME, dstouch_dsname, dstouch_volser);
            return -1;
        }

        /* check if member name is specified but dataset is not a PDS */
        if( dstouch_member[0] && ! S_ISDIR(filestat.st_mode) ) {
        /*  OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "dstouch_member", retval); */
            sprintf(ret_msg, "%s: dataset is not a PDS while member name is specified - dsname=%s\n", SERVICE_NAME, dstouch_dsname);
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
        if( ! dstouch_catalog[0] ) strcpy(dstouch_catalog, result[0].catname);
        /* if( ! dstouch_volser[0] ) strcpy(dstouch_volser, nvsm_info.volser); */

        /* check if catalog information is different */
        if( dstouch_volser[0] && strcmp(dstouch_volser, nvsm_info.volser) ) {
        /*  OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "dstouch_volser", retval); */
            sprintf(ret_msg, "%s: volume serial information is different from the catalog - dsname=%s\n", SERVICE_NAME, dstouch_dsname);
            return -1;
        }

        /* check if member name is specified but dataset is not a PDS */
        if( dstouch_member[0] && strncmp(nvsm_info.dsorg, "PO", 2) ) {
        /*  OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "dstouch_member", retval); */
            sprintf(ret_msg, "%s: dataset is not a PDS while member name is specified - dsname=%s\n", SERVICE_NAME, dstouch_dsname);
            return -1;
        }

    /* otherwise dataset is VSAM */
    } else {
        /* check if user catalog is not specified */
        if( ! dstouch_catalog[0] ) strcpy(dstouch_catalog, result[0].catname);
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
    retval = ams_use_catalog(dstouch_catalog);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "ams_use_catalog", retval);
        sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "ams_use_catalog", retval);
        goto _TOUCH_DATASET_ERR_RETURN_00;
    }

    /* prepare allocation dsname */
    if (dstouch_member[0] != 0) sprintf(dsname_conv, "%s(%s)", dstouch_dsname, dstouch_member);
    else strcpy(dsname_conv, dstouch_dsname);

    /* prepare allocation request */
    memset(&req, 0x00, sizeof(req));
    req.disp.status = DISP_STATUS_SHR;
    req.disp.normal = DISP_TERM_KEEP;
    req.disp.abnormal = DISP_TERM_KEEP;

    /* prepare request - volume */
    strcpy(req.volume.unit, "");
    strcpy(req.volume.vlist, dstouch_volser);

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
    if (dstouch_credt[0])
    {
        if( dsalc_new_allocate_method && dstouch_volser[0] )
        {
            retval = AMS_ERR_NOT_FOUND;
        }
        else
        {
            /* search catalog entries */
            retval = ams_search_entries(dstouch_dsname, "ACGHU", &rcount, &result, AMS_SEARCH_DEFAULT);
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
            retval = ams_touch_nvsm_ds(dstouch_dsname, dstouch_volser, dstouch_credt, AMS_TOUCH_DEFAULT);
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
            retval = ams_touch(result.catname, result.entname, result.enttype, dstouch_credt, AMS_TOUCH_DEFAULT);
            if (retval < 0)
            {
                OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "ams_touch", retval);
                sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "ams_touch", retval);
                goto _TOUCH_DATASET_ERR_RETURN_04;
            }
             /* ams_touch() doesn't support touching pds member */
            if (dstouch_member[0])
            {/* retrieve information from catalog */
                retval = ams_info(result.catname, result.entname, result.enttype, & nvsm_info, AMS_INFO_DEFAULT);
                 if( retval < 0 )
                 {
                    OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "ams_info", retval);
                    sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "ams_info", retval);
                    goto _TOUCH_DATASET_ERR_RETURN_04;
                 }
                 /* call ams function to update pds member creation date */
                 retval = ams_touch_nvsm_ds(dsname_conv, nvsm_info.volser, dstouch_credt, AMS_TOUCH_DEFAULT);
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
    retval = dsio_dcb_volser(dcbs, 0, dstouch_volser);
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
