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
 *      OFRUISVRDSSAVE
 *
 * Description:
 *      Dataset save program for external editor.
 *
 * Input:
 *      FB_TACF_TOKEN(string): TACF token
 *      FB_DSNAME(string): dataset name
 *      FB_MEMNAME(string): member name
 *      FB_CATNAME(string): catalog name
 *      FB_FILEPATH(string): dataset filepath
 *      FB_TYPE(char): save options (multiple, optional)
 *      FB_ARGS(string): delimiter from tool
 *
 * Format(FB_TYPE):
 *      L: use the delimiter to separate each record
 *      B: fills in spaces if a record is shorter 
 *      R: removes the source file after saving
 *
 * Output:
 *      FB_RETMSG(string): error message
 */

#define SERVICE_NAME    "OFRUISVRDSSAVE"

static char dssave_dataset[DS_DSNAME_LEN + 2];
static char dssave_member[DS_MEMBER_LEN2 + 2];
static char dssave_catalog[DS_DSNAME_LEN + 2];
static char dssave_srcpath[256];
static char dssave_deli_form[256], delimiter[256];
static int dssave_fill_spaces;
static int dssave_remove_source;
static int dssave_test_mode;

static int _get_params(FBUF *rcv_buf);
static int _validate_param(char *ret_msg);
static int _adjust_param();
static int _convert_delim();
static int _init_libraries();
static int _final_libraries();
static int _check_src_exist(char *ret_msg);
static int _check_des_exist(char *ret_msg);
static int _dssave_dataset(char *ret_msg);
static int _check_input_file(char *filepath, char *recfm, int maxlen, char *delim, char *ret_msg);
static int _dssave_dataset_inner(int fdin, int fdout, int dcb_type, char* dsorg, int maxlen, char *delim);


void OFRUISVRDSSAVE(TPSVCINFO *tpsvcinfo)
{
    int     retval;
    FBUF   *rcv_buf = NULL;
    FBUF   *snd_buf = NULL;
    char    ret_msg[1024] = {'\0',};

    /* service start */
    retval = svrcom_svc_start(SERVICE_NAME);
    if (retval != SVRCOM_ERR_SUCCESS) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_SVRCOM_FUNCTION_ERROR, SERVICE_NAME, "svrcom_svc_start", retval);
        goto _DSSAVE_MAIN_ERR_TPFAIL_00;
    }

    /* get rcv buffer */
    rcv_buf = (FBUF *)(tpsvcinfo->data);
    fbprint(rcv_buf);
    
    /* allocate snd buffer */
    snd_buf = (FBUF *)tpalloc("FIELD", NULL, 0);
    if (snd_buf == NULL) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_TMAX_TPALLOC_ERROR, SERVICE_NAME, "snd_buf", tpstrerror(tperrno));
        retval = SVRCOM_ERR_TPALLOC; goto _DSSAVE_MAIN_ERR_TPFAIL_00;
    }
   
    /* initialize parameter */
    memset(dssave_dataset,0x00,sizeof(dssave_dataset));
    memset(dssave_member,0x00,sizeof(dssave_member));
    memset(dssave_catalog,0x00,sizeof(dssave_catalog));
    memset(dssave_srcpath,0x00,sizeof(dssave_srcpath));
    memset(dssave_deli_form,0x00,sizeof(dssave_deli_form));

    /* initialize parameter2 */    
    dssave_fill_spaces = 0;
    dssave_remove_source = 0;
    dssave_test_mode = 0;
   
    /* get parameters */
    retval = _get_params(rcv_buf);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_INTERNAL_FUNC_ERROR, SERVICE_NAME, "_get_params", retval);
        sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "_get_params", retval);
        goto _DSSAVE_MAIN_ERR_TPFAIL_01;
    }

    /* validate parameters */
    retval = _validate_param(ret_msg);
    if (retval < 0 ) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_INTERNAL_FUNC_ERROR, SERVICE_NAME, "_validate_params", retval);
        goto _DSSAVE_MAIN_ERR_TPFAIL_01;
    }

    /* adjust parameters */
    retval = _adjust_param();
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_INTERNAL_FUNC_ERROR, SERVICE_NAME, "_adjust_param", retval);
        sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "_adjust_param", retval);
        goto _DSSAVE_MAIN_ERR_TPFAIL_01;
    }

    /* print save parameters */
    OFCOM_MSG_PRINTF2(UISVR_MSG_SOURCE_FILE, SERVICE_NAME, dssave_srcpath);
    OFCOM_MSG_PRINTF2(UISVR_MSG_DEST_DATASET, SERVICE_NAME, dssave_dataset);
    OFCOM_MSG_PRINTF2(UISVR_MSG_DEST_MEMBER, SERVICE_NAME, dssave_member);
    OFCOM_MSG_PRINTF2(UISVR_MSG_USER_CATALOG, SERVICE_NAME, dssave_catalog);
    OFCOM_MSG_PRINTF2(UISVR_MSG_DELIMITER, SERVICE_NAME, dssave_deli_form);

    /* convert delimiter */
    retval = _convert_delim();
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_INTERNAL_FUNC_ERROR, SERVICE_NAME, "_convert_delim", retval);
        sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "_convert_delim", retval);
        goto _DSSAVE_MAIN_ERR_TPFAIL_01;
    }

    /* uisvr login process */
    retval = uisvr_login_process(SERVICE_NAME, rcv_buf);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_INNER_FUNCTION_ERROR, SERVICE_NAME, "uisvr_login_process", retval);
        sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "uisvr_login_process", retval);
        goto _DSSAVE_MAIN_ERR_TPFAIL_01;
    }

    /* library initialization */
    retval = _init_libraries();
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_INTERNAL_FUNC_ERROR, SERVICE_NAME, "_init_libraries", retval);
        sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "_init_libraries", retval);
        goto _DSSAVE_MAIN_ERR_TPFAIL_02;
    }

    /* check if file exist */
    retval = _check_src_exist(ret_msg);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_INTERNAL_FUNC_ERROR, SERVICE_NAME, "_check_src_exist", retval);
    /*  sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "_check_src_exist", retval); -- composed already */
        goto _DSSAVE_MAIN_ERR_TPFAIL_03;
    }

    /* check if destination exist */
    retval = _check_des_exist(ret_msg);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_INTERNAL_FUNC_ERROR, SERVICE_NAME, "_check_des_exist", retval);
        goto _DSSAVE_MAIN_ERR_TPFAIL_03;
    }

    /* save dataset */
    retval = _dssave_dataset(ret_msg);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_INTERNAL_FUNC_ERROR, SERVICE_NAME, "_dssave_dataset", retval);
    /*  sprintf(ret_msg, "%s: %s() failed. rc(%d)\n", SERVICE_NAME, "_dssave_dataset", retval); -- composed already */
        goto _DSSAVE_MAIN_ERR_TPFAIL_03;
    }

    /* print message */
    OFCOM_MSG_PRINTF2(UISVR_MSG_DS_SAVE_OK, SERVICE_NAME, dssave_dataset);

    /* check to remove source */
    if (dssave_remove_source && ! dssave_test_mode) {
        OFCOM_MSG_PRINTF2(UISVR_MSG_REMOVING_SOURCE, SERVICE_NAME, dssave_srcpath);
        unlink(dssave_srcpath);
    }

    /* finalize libraries */
    _final_libraries();
    uisvr_logout_process();

    /* fbput ret_msg */
    sprintf(ret_msg, "%s: Dataset Is Saved Successfully\n", SERVICE_NAME);
    retval = svrcom_fbput(snd_buf, FB_RETMSG, ret_msg, 0);

    /* service end */
    svrcom_svc_end(TPSUCCESS, 0);
    tpreturn(TPSUCCESS, 0, (char *)snd_buf, 0, TPNOFLAGS);

_DSSAVE_MAIN_ERR_TPFAIL_03:
    _final_libraries();

_DSSAVE_MAIN_ERR_TPFAIL_02:
    uisvr_logout_process();

_DSSAVE_MAIN_ERR_TPFAIL_01:
    if ( snd_buf ) svrcom_fbput(snd_buf, FB_RETMSG, ret_msg, 0);

_DSSAVE_MAIN_ERR_TPFAIL_00:
    svrcom_svc_end(TPFAIL, retval);
    UISVR_TPRETURN_TPFAIL_CHECK_DISCONNECTED(retval, (char *)snd_buf, 0, TPNOFLAGS);
}


static int _get_params(FBUF *rcv_buf)
{
    int  retval;
    char save_type;

    /* fbget dataset name */
    retval = svrcom_fbget(rcv_buf, FB_DSNAME, dssave_dataset, DS_DSNAME_LEN); 
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_SVRCOM_FUNCTION_ERROR, SERVICE_NAME, "svrcom_fbget", retval);
        return retval;
    }

    /* fbget member name */
    if( dscom_is_relaxed_member_limit() )
        retval = svrcom_fbget(rcv_buf, FB_MEMNAME, dssave_member, DS_MEMBER_LEN2);
    else
        retval = svrcom_fbget(rcv_buf, FB_MEMNAME, dssave_member, DS_MEMBER_LEN);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_SVRCOM_FUNCTION_ERROR, SERVICE_NAME, "svrcom_fbget", retval);
        return retval;
    }

    /* fbget user catalog */
    retval = svrcom_fbget_opt(rcv_buf, FB_CATNAME, dssave_catalog, DS_DSNAME_LEN); 
    if (retval < 0 && retval != SVRCOM_ERR_FBNOENT) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_SVRCOM_FUNCTION_ERROR, SERVICE_NAME, "svrcom_fbget", retval);
        return retval;
    } else if (retval == SVRCOM_ERR_FBNOENT) {
        dssave_catalog[0] = '\0';
    }

    /* fbget dataset filepath */
    retval = svrcom_fbget(rcv_buf, FB_FILEPATH, dssave_srcpath, sizeof(dssave_srcpath) - 1); 
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_TMAX_FBGET_ERROR, SERVICE_NAME, "FB_FILEPATH", fbstrerror(fberror));
        return retval;
    }
    
    /* fbget dssave_deli_form from FB_ARGS */
    retval = svrcom_fbget_opt(rcv_buf, FB_ARGS, dssave_deli_form, sizeof(dssave_deli_form) - 1);
    if (retval < 0 && retval != SVRCOM_ERR_FBNOENT) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_SVRCOM_FUNCTION_ERROR, SERVICE_NAME, "svrcom_fbget_opt", retval);
        return retval;
    }

    /* fbget multiple options from FB_TYPE */
    while(1)
    {
        retval = svrcom_fbget_opt(rcv_buf, FB_TYPE, &save_type, 1);
        if (retval < 0 )
        {
            if (retval != SVRCOM_ERR_FBNOENT)
            {
                OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_SVRCOM_FUNCTION_ERROR, SERVICE_NAME, "svrcom_fbget", retval);
                return retval;
            }
            break;
        }
        if (save_type == 'L' || save_type == 'l')
            strcpy(dssave_deli_form, "\\n");
        else if (save_type == 'B' || save_type == 'b')
            dssave_fill_spaces = 1;
        else if (save_type == 'R' || save_type == 'r')
            dssave_remove_source = 1;
        else if (save_type == 'T' || save_type == 't')
            dssave_test_mode = 1;
    }

    return 0;
}

static int _validate_param(char *ret_msg)
{
    int retval;

    /* check if dataset name is specified */
    if (dssave_dataset[0] != '\0') {
        /* check if dataset name is invalid */
        retval = dscom_check_dsname(dssave_dataset);
        if (retval < 0) {
            OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_INVALID_DSN_ERROR, SERVICE_NAME, dssave_dataset);
            sprintf(ret_msg, "%s: invalid dataset name. dsname=%s\n", SERVICE_NAME, dssave_dataset);
    
            return retval;
        }
    }

    /* check if member name is specified */
    if (dssave_member[0] != '\0') {
        /* check if member name is invalid */
        retval = dscom_check_dsname2(dssave_dataset, dssave_member);
        if (retval < 0) {
            OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_INVALID_MEMBER_ERROR, SERVICE_NAME, dssave_member);
            sprintf(ret_msg, "%s: invalid member name. member=%s\n", SERVICE_NAME, dssave_member);

            return retval;
        }
    }

    /* check if user catalog is specified */
    if (dssave_catalog[0] != '\0') {
        /* check if catalog name is invalid */
        retval = dscom_check_dsname(dssave_catalog);
        if (retval < 0) {
            OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_INVALID_CATNAME_ERROR, SERVICE_NAME, dssave_catalog);
            sprintf(ret_msg, "%s: invalid catalog name. catalog=%s\n", SERVICE_NAME, dssave_catalog);

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
    if (dssave_srcpath[0] == 0x00) {
        retval = ofcom_conf_get_value("dstool.conf", "DSLOAD", "LOAD_DIR", s_temp, sizeof(s_temp));
        if (retval < 0) {
            OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_OFCOM_FUNCTION_ERROR, SERVICE_NAME, "ofcom_conf_get_value", retval);
            return retval;
        }

        strcpy(dssave_srcpath, s_temp);
        strcat(dssave_srcpath, "/");
        strcat(dssave_srcpath, dssave_dataset);

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


static int _convert_delim()
{
    int  length, i;
    char ch, *ptr;

    /* initialize delimiter */
    memset(delimiter,0x00,sizeof(delimiter));
    ptr = delimiter;

    /* convert delimiter */
    if (dssave_deli_form[0]) {
        length = strlen(dssave_deli_form);
        for( i = 0; i < length; i++ ) {
            if( dssave_deli_form[i] == '\\' && i+1 < length ) {
                i++; ch = dssave_deli_form[i];
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
            } else *ptr++ = dssave_deli_form[i];
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
    int fd;

    /* check to see if file exist */
    fd = open(dssave_srcpath, O_RDONLY);
    if (fd < 0) {
        OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_FILE_NOT_FOUND_ERROR, SERVICE_NAME, dssave_srcpath);
        sprintf(ret_msg, "%s: file %s is not found.\n", SERVICE_NAME, dssave_srcpath);
        return SVRCOM_ERR_INVALID_PARAM;
    }
    close(fd);

    return 0;
}


static int _check_des_exist(char *ret_msg)
{
    int retval, rcount = 1; icf_result_t result[1]; int search_flags;
    ams_info_nvsm_t nvsm_info;
    char *cutpos; 

    /* call a function to set search catalog */
    retval = ams_use_catalog(dssave_catalog);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "ams_use_catalog", retval); 
        sprintf(ret_msg, "%s: %s() failed. rc(%d)\n", SERVICE_NAME, "ams_use_catalog", retval);
        return retval;
    }

    /* set search options for ams_search_entries() */
    if (dssave_catalog[0]) search_flags = AMS_SEARCH_1_CATALOG;
    else search_flags = AMS_SEARCH_DEFAULT;

    /* check if wild card is used */
    if (strchr(dssave_dataset, '*') || strchr(dssave_dataset, '%')) {
        OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_NO_WILD_CARD_ALLOWD_ERROR, SERVICE_NAME, dssave_dataset);
        sprintf(ret_msg, "%s: wild card character is not allowed in dsname - dsname=%s\n", SERVICE_NAME, dssave_dataset)    ;
        return -1;
    }
    
    /* separate member name from dsname */
    if ((cutpos = strchr(dssave_dataset, '('))) {
        *cutpos = '\0'; strcpy(dssave_member, cutpos+1);
        if ((cutpos = strchr(dssave_member, ')'))) *cutpos = '\0';
    }

    /* search source dataset in the catalog */
    retval = ams_search_entries(dssave_dataset, "AH", &rcount, result, search_flags);
    if (retval < 0 && retval != AMS_ERR_NOT_FOUND) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "ams_search_entries", retval);
        sprintf(ret_msg, "%s: %s() failed. rc(%d)\n", SERVICE_NAME, "ams_search_entries", retval);
        return retval;
    }

    /* check if dataset is not found */
    if (retval == AMS_ERR_NOT_FOUND) {
        /* check if catalog name is specified */
        if (dssave_catalog[0]) {
            OFCOM_MSG_FPRINTF4(stderr, UISVR_MSG_NO_DSNAME_IN_CATALOG_ERROR, SERVICE_NAME, SERVICE_NAME, dssave_dataset, dssave_catalog);
            sprintf(ret_msg, "dataset is not found in the catalog - dsname=%s,catalog=%s\n", dssave_dataset, dssave_catalog);

            return retval;
        }

    /* check if member name is specified but dataset is not a PDS */
    } else if (AMS_IS_ENTRY_NONVSAM(result[0].enttype)) {
        /* retrieve information from catalog */
        retval = ams_info(result[0].catname, result[0].entname, result[0].enttype, &nvsm_info, AMS_INFO_DEFAULT);
        if (retval < 0) {
            OFCOM_MSG_FPRINTF4(stderr, UISVR_MSG_NO_DSNAME_IN_CATALOG_ERROR, SERVICE_NAME, SERVICE_NAME, dssave_dataset, dssave_catalog);
            sprintf(ret_msg, "%s: dataset is not found in the catalog - dsname=%s,catalog=%s\n", SERVICE_NAME, dssave_dataset, dssave_catalog);

            return retval;
        }

        /* check if user catalog is not specified */
        if (!dssave_catalog[0]) strcpy(dssave_catalog, result[0].catname);

        /* check if member name is specified but dataset is not a PDS */
        if (dssave_member[0] && strncmp(nvsm_info.dsorg, "PO", 2)) {
            OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_DATASET_IS_NOT_PDS_ERROR, SERVICE_NAME, dssave_dataset);
            fprintf(stderr, "%s: dataset is not a PDS while member name - dsname=%s\n", SERVICE_NAME, dssave_dataset);
            return -1;
        }


    /* otherwise dataset is VSAM */
    } else {
        /* check if user catalog is not specified */
        if (!dssave_catalog[0]) strcpy(dssave_catalog, result[0].catname);
    }

    return 0;

}


static int _dssave_dataset(char *ret_msg)
{
    int retval, fdin, fdout, dcb_type, maxlen, blksize;
    char s_composed[256], dsorg[8], recfm[8];
    
    int handle = 0;
    dsalc_req_t req;

    /* set user catalog */
    retval = ams_use_catalog(dssave_catalog);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "ams_use_catalog", retval);
        sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "ams_use_catalog", retval);
        goto _DSSAVE_DATASET_ERR_RETURN_00;
    }

    /* dsalc_allocate */
    memset(&req, 0x00, sizeof(req));
    req.disp.status = DISP_STATUS_OLD;
    req.disp.normal = DISP_TERM_KEEP;
    req.disp.abnormal = DISP_TERM_KEEP;

    /* dataset name */
    if (dssave_member[0]) {
        sprintf(s_composed, "%s(%s)", dssave_dataset, dssave_member);
    } else {
        strcpy(s_composed, dssave_dataset);
    }

    /* set lock wait flag */
    req.lock.lock_wait = LOCKM_LOCK_WAIT_IMMEDIATE;

    /* allocate dataset */    
    handle = dsalc_allocate("DSSAVE", s_composed, &req, DSALC_ALLOCATE_DEFAULT);
    if (handle < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_DSALC_FUNCTION_ERROR, SERVICE_NAME, "dsalc_allocate", handle);
        sprintf(ret_msg, "%s: %s() failed. rc(%d)\n", SERVICE_NAME, "dsalc_allocate", handle);
        retval = handle; goto _DSSAVE_DATASET_ERR_RETURN_00;
    }

    /* get dcb_type */
    retval = dsio_dcb_get_type(dsalc_get_dcbs(handle), & dcb_type, NULL);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_DSIO_FUNCTION_ERROR, SERVICE_NAME, "dsio_dcb_get_type", retval);
        sprintf(ret_msg, "%s: %s() failed. rc(%d)\n", SERVICE_NAME, "dsio_dcb_get_type", retval);
        goto _DSSAVE_DATASET_ERR_RETURN_03;
    }

    /* check VSAM type */
    if (dcb_type != DSIO_DCB_TYPE_NVSM && dcb_type != DSIO_DCB_TYPE_ISAM && dcb_type != DSIO_DCB_TYPE_BDAM) {
        OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_TSAM_NOT_SUPPORT_ERROR, SERVICE_NAME, s_composed);
        sprintf(ret_msg, "%s: TSAM dataset is not supported. dsname=%s\n", SERVICE_NAME, s_composed);
        retval = -1; goto _DSSAVE_DATASET_ERR_RETURN_03;
    }

    /* get maximum length */
    retval = dsio_dcb_maxlrecl(dsalc_get_dcbs(handle), & maxlen, NULL);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_DSIO_FUNCTION_ERROR, SERVICE_NAME, "dsio_dcb_maxlrecl", retval);
        sprintf(ret_msg, "%s: %s() failed. rc(%d)\n", SERVICE_NAME, "dsio_dcb_maxlrecl", retval);
        goto _DSSAVE_DATASET_ERR_RETURN_03;
    }

    /* get blksize */
    retval = dsio_dcb_cisize(dsalc_get_dcbs(handle), 0, & blksize, NULL);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_DSIO_FUNCTION_ERROR, SERVICE_NAME, "dsio_dcb_cisize", retval);
        sprintf(ret_msg, "%s: %s() failed. rc(%d)\n", SERVICE_NAME, "dsio_dcb_cisize", retval);
        goto _DSSAVE_DATASET_ERR_RETURN_03;
    }

    /* get nvsm info */
    retval = dsio_dcb_nvsm_info(dsalc_get_dcbs(handle), dsorg, recfm);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_DSIO_FUNCTION_ERROR, SERVICE_NAME, "dsio_dcb_nvsm_info", retval);
        sprintf(ret_msg, "%s: %s() failed. rc(%d)\n", SERVICE_NAME, "dsio_dcb_nvsm_info", retval);
        goto _DSSAVE_DATASET_ERR_RETURN_03;
    }

    /* process spaces in case of FB */
    if (strncmp(recfm, "FB", 2) != 0) dssave_fill_spaces = 0;

    /* check input file */
    retval = _check_input_file(dssave_srcpath, recfm, (dcb_type == DSIO_DCB_TYPE_BDAM) ? blksize : maxlen, delimiter, ret_msg);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_INTERNAL_FUNC_ERROR, SERVICE_NAME, "_check_input_file", retval);
    /*  sprintf(ret_msg, "%s: %s() failed. rc(%d)\n", SERVICE_NAME, "_check_input_file", retval); -- composed already */
        goto _DSSAVE_DATASET_ERR_RETURN_03;
    }

    /* check not test mode */
    if (!dssave_test_mode) {
        /* open input file */
        fdin = open(dssave_srcpath, O_RDONLY);
        if (fdin < 0) {
            OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_FILE_OPEN_ERROR, SERVICE_NAME, dssave_srcpath, strerror(errno));
            sprintf(ret_msg, "%s: file open error. path=%s,err=%s\n", SERVICE_NAME, dssave_srcpath, strerror(errno));
            retval = fdin; goto _DSSAVE_DATASET_ERR_RETURN_03;
        }

        /* dsio_batch_open */
        fdout = dsio_batch_open(dsalc_get_dcbs(handle), DSIO_OPEN_OUTPUT | DSIO_ACCESS_SEQUENTIAL | DSIO_LOCK_EXCLUSIVE);
        if (fdout < 0) {
            OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_DSIO_FUNCTION_ERROR, SERVICE_NAME, "dsio_batch_open", retval);
            sprintf(ret_msg, "%s: %s() failed. rc(%d)\n", SERVICE_NAME, "dsio_batch_open", fdout);
            retval = fdout; goto _DSSAVE_DATASET_ERR_RETURN_04;
        }

        /* dssave_dataset_inner */
        retval = _dssave_dataset_inner(fdin, fdout, dcb_type, dsorg,  (dcb_type == DSIO_DCB_TYPE_BDAM) ? blksize : maxlen, delimiter);
        if (retval < 0) {
            OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_INTERNAL_FUNC_ERROR, SERVICE_NAME, "_dssave_dataset_inner", retval);
            sprintf(ret_msg, "%s: %s() failed. rc(%d)\n", SERVICE_NAME, "_dssave_dataset_inner", retval);
            goto _DSSAVE_DATASET_ERR_RETURN_05;
        }

        /* dsio_batch_close */
        retval = dsio_batch_close(fdout, DSIO_CLOSE_DEFAULT);
        if (retval < 0) {
            OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_DSIO_FUNCTION_ERROR, SERVICE_NAME, "dsio_batch_close", retval);
            sprintf(ret_msg, "%s: %s() failed. rc(%d)\n", SERVICE_NAME, "dsio_batch_close", retval);
            goto _DSSAVE_DATASET_ERR_RETURN_04;
        }

        /* close input file */
        retval = close(fdin);
        if (retval < 0) {
            OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_FILE_CLOSE_ERROR, SERVICE_NAME, fdin, strerror(errno));
            sprintf(ret_msg, "%s: file close error. fd(%d),err(%s)\n", SERVICE_NAME, fdin, strerror(errno));
            goto _DSSAVE_DATASET_ERR_RETURN_03;
        }
    }
    /* dsalc_dispose */
    retval = dsalc_dispose(handle, DISP_COND_NORMAL, DSALC_DISPOSE_DEFAULT);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_DSALC_FUNCTION_ERROR, SERVICE_NAME, "dsalc_dispose", retval);
        sprintf(ret_msg, "%s: %s() failed. rc(%d)\n", SERVICE_NAME, "dsalc_dispose", retval);
        goto _DSSAVE_DATASET_ERR_RETURN_03;
    }

    /* dsalc_unallocate */
    retval = dsalc_unallocate(handle, DISP_COND_NORMAL, DSALC_UNALLOCATE_DEFAULT);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_DSALC_FUNCTION_ERROR, SERVICE_NAME, "dsalc_unallocate", retval);
        sprintf(ret_msg, "%s: %s() failed. rc(%d)\n", SERVICE_NAME, "dsalc_unallocate", retval);
        goto _DSSAVE_DATASET_ERR_RETURN_00;
    }

    return 0;

_DSSAVE_DATASET_ERR_RETURN_05:
    dsio_batch_close(fdout, DSIO_CLOSE_DEFAULT);

_DSSAVE_DATASET_ERR_RETURN_04:
    close(fdin);

_DSSAVE_DATASET_ERR_RETURN_03:
    dsalc_unallocate(handle, DISP_COND_ABNORMAL, DSALC_UNALLOCATE_DEFAULT);

_DSSAVE_DATASET_ERR_RETURN_00:
    return retval;
}


static int _check_input_file(char *filepath, char *recfm, int maxlen, char *delim, char *ret_msg)
{
    int retval;
    int i, delim_size, fd, n_read, buflen, reclen;
    char *buf = NULL; int fixed_length = 0, rec_count = 0;

    struct stat filestat;

    delim_size = strlen(delim);

    fd = open(filepath, O_RDONLY);
    if( fd < 0 ) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_FILE_OPEN_ERROR, SERVICE_NAME, filepath, strerror(errno));
        sprintf(ret_msg, "%s: file open error. path=%s,err=%s\n", SERVICE_NAME, filepath, strerror(errno));
        return -1;
    }

    /* check if fixed length is used */
    if( strncmp(recfm, "FB", 2) == 0 ) fixed_length = 1;

    if( delim_size > 0 ) {
        buf = malloc(maxlen + delim_size + 1);
        if( ! buf ) {
            OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_MALLOC_ERROR, SERVICE_NAME, "buf", strerror(errno));
            sprintf(ret_msg, "%s: malloc for %s failed. err=%s\n", SERVICE_NAME, "buf", strerror(errno));
            close(fd); return -1;
        }

        buflen = 0;
        while( 1 ) {
            n_read = read(fd, buf + buflen, maxlen + delim_size - buflen);
            if( n_read < 0 ) {
                OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_FILE_READ_ERROR, SERVICE_NAME, fd, strerror(errno));
                sprintf(ret_msg, "%s: file read error. fd=%d,err=%s\n", SERVICE_NAME, fd, strerror(errno));
                free(buf); close(fd); return -1;
            }

            if( n_read == 0 ) break;
            else buflen += n_read;

            i = 0;
            while( i < buflen ) {
                if( i >= (delim_size - 1) && ! memcmp(buf + i - (delim_size - 1), delim, delim_size) ) {
                /*  memcpy(rec, buf, i - (delim_size - 1)); */
                    reclen = i - (delim_size - 1);

                    if( reclen > maxlen ) {
                        OFCOM_MSG_FPRINTF4(stderr, UISVR_MSG_RECORD_LENGTH_ERROR, SERVICE_NAME, rec_count, reclen, maxlen);
                        sprintf(ret_msg,"%s: input record is longer than the maximum length. record=%d,reclen=%d,maxlen=%d\n", SERVICE_NAME, rec_count, reclen, maxlen);
                        free(buf); close(fd); return -1;

                    } else if( reclen < maxlen && ! dssave_fill_spaces && fixed_length ) {
                        OFCOM_MSG_FPRINTF4(stderr, UISVR_MSG_RECORD_LENGTH_ERROR, SERVICE_NAME, rec_count, reclen, maxlen);
                        sprintf(ret_msg,"%s: input record is shorter than the fixed length. record=%d,reclen=%d,maxlen=%d\n", SERVICE_NAME, rec_count, reclen, maxlen);
                        free(buf); close(fd); return -1;
                    }

                    rec_count++;

                    memmove(buf, buf + i + 1, buflen - (i + 1));
                    buflen = buflen - (i + 1);

                    i = 0;
                    continue;
                }

                i++;
            }
        }

        if( buflen > 0 ) {
            OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_RECORD_DELIMITER_ERROR, SERVICE_NAME, rec_count);
            sprintf(ret_msg,"%s: input record does not have a delimiter. record=%d\n", SERVICE_NAME, rec_count);
            free(buf); close(fd); return -1;
        }

        free(buf);
    }
    // delimiter is NULL string
    else {
        if (fixed_length && !dssave_fill_spaces) {
            retval = fstat(fd, &filestat);
            if (retval < 0) {
                OFCOM_MSG_FPRINTF4(stderr, UISVR_MSG_STAT_FUNC_ERROR, SERVICE_NAME, filepath, strerror(errno), retval);
                sprintf(ret_msg,"%s: fstat() function failed. filepath=%s, errno=%d\n", SERVICE_NAME, filepath, errno);
                close(fd); return -1;
            }

            reclen = filestat.st_size % maxlen;
            if (reclen) {
                OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_RECORD_LENGTH_ERROR, SERVICE_NAME, reclen, maxlen);
                sprintf(ret_msg,"%s: last record is shorter than the fixed length. reclen=%d,maxlen=%d\n", SERVICE_NAME, reclen, maxlen);
                close(fd); return -1;
            }
        }
    }

    close(fd);
    return 0;
}


static int _dssave_dataset_inner(int fdin, int fdout, int dcb_type, char* dsorg, int maxlen, char *delim)
{
    int retval, i, delim_size, n_read, buflen, reclen;
    char *rec = NULL, *buf = NULL; int rec_count = 0;
    int dsio_flag = 0, rrn = 0;

    if( dcb_type == DSIO_DCB_TYPE_BDAM ) {
        dsio_flag = DSIO_FLAG_RRN;
    } else if( dcb_type == DSIO_DCB_TYPE_ISAM ) {
        dsio_flag = DSIO_FLAG_REC;
    }

    delim_size = strlen(delim);

    rec = malloc(maxlen + 1);
    if( ! rec ) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_MALLOC_ERROR, SERVICE_NAME, "rec", strerror(errno));
        return -1;
    }

    if( delim_size > 0 ) {
        buf = malloc(maxlen + delim_size + 1);
        if( ! buf ) {
            OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_MALLOC_ERROR, SERVICE_NAME, "buf", strerror(errno));
            free(rec); return -1;
        }

        buflen = 0;
        while( 1 ) {
            n_read = read(fdin, buf + buflen, maxlen + delim_size - buflen);
            if( n_read < 0 ) {
                OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_FILE_READ_ERROR, SERVICE_NAME, fdin, strerror(errno));
                free(buf); free(rec); return -1;
            }

            if( n_read == 0 ) break;
            else buflen += n_read;

            i = 0;
            while( i < buflen ) {
                if( i >= (delim_size - 1) && ! memcmp(buf + i - (delim_size - 1), delim, delim_size) ) {
                    memcpy(rec, buf, i - (delim_size - 1));
                    reclen = i - (delim_size - 1);

                    if( reclen < maxlen && dssave_fill_spaces ) {
                        memset(rec + reclen, ' ', maxlen - reclen);
                        reclen = maxlen;
                    }

                    /* write data */
                    if( ((dcb_type == DSIO_DCB_TYPE_NVSM) && !(strcmp(dsorg, "DA"))) ||
                        (dcb_type == DSIO_DCB_TYPE_BDAM)) {
                        retval = dsio_batch_write(fdout, 0, &rrn, 0, rec, reclen, dsio_flag);
                        if( retval < 0 ) {
                            OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_DSIO_FUNCTION_ERROR, SERVICE_NAME, "dsio_batch_write", retval);
                            free(buf); free(rec); return retval;
                        }

                        /* increase rrn */
                        rrn++;
                    } else {
                        retval = dsio_batch_write(fdout, 0, NULL, 0, rec, reclen, dsio_flag);
                        if( retval < 0 ) {
                            OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_DSIO_FUNCTION_ERROR, SERVICE_NAME, "dsio_batch_write", retval);
                            free(buf); free(rec); return retval;
                        }
                    }
                    
                    /* increase rec_count */
                    rec_count++;

                    memmove(buf, buf + i + 1, buflen - (i + 1));
                    buflen = buflen - (i + 1);

                    i = 0;
                    continue;
                }

                i++;
            }
        }

        free(buf);

    } else {
        while( 1 ) {
            n_read = read(fdin, rec, maxlen);
            if( n_read < 0 ) {
                OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_FILE_READ_ERROR, SERVICE_NAME, fdin, strerror(errno));
                free(rec); return -1;
            }

            if( n_read == 0 ) break;
            else reclen = n_read;

            if (reclen < maxlen && dssave_fill_spaces) {
                memset(rec + reclen, ' ', maxlen - reclen);
                reclen = maxlen;
            }
            
            /* write data */
            if( ((dcb_type == DSIO_DCB_TYPE_NVSM) && !(strcmp(dsorg, "DA"))) ||
                (dcb_type == DSIO_DCB_TYPE_BDAM)) {
                retval = dsio_batch_write(fdout, 0, &rrn, 0, rec, reclen, dsio_flag);
                if( retval < 0 ) {
                    OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_DSIO_FUNCTION_ERROR, SERVICE_NAME, "dsio_batch_write", retval);
                    free(buf); free(rec); return retval;
                }

                /* increase rrn */
                rrn++;
            } else {
                retval = dsio_batch_write(fdout, 0, NULL, 0, rec, reclen, dsio_flag);
                if( retval < 0 ) {
                    OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_DSIO_FUNCTION_ERROR, SERVICE_NAME, "dsio_batch_write", retval);
                    free(buf); free(rec); return retval;
                }
            }
            
            /* increase rec_count */
            rec_count++;
        }
    }

    free(rec);
    return 0;
}
