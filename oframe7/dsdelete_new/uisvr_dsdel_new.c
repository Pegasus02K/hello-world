#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <ctype.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <usrinc/atmi.h>
#include <usrinc/fbuf.h>

#include "uisvr.h"
#include "ofcom.h"
#include "svrcom.h"
#include "volm.h"
#include "ams.h"
#include "amsx.h"

#include "oframe_fdl.h"
#include "msgcode_uisvr.h"

/*
 * Service Name:
 *      OFRUISVRDSDEL
 *
 * Description:
 *      Delete an existing dataset.
 *
 * Input:
 *      FB_TACF_TOKEN(string): TACF token
 *      FB_DSNAME(string): dataset name
 *      FB_CATNAME(string): catalog name (optional)
 *      FB_VOLUME(string): volume serial (optional)
 *      FB_TYPE(char): delete options (optional)
 *
 * Format(FB_TYPE):
 *      F: delete catalog entry even if the dataset does not exist (force)
 *      I: ignore parameter validation check
 *      U: delete only the catalog entry for the dataset (uncatalog)
 *
 * Output
 *      FB_RETMSG(string): error message
 */

#define SERVICE_NAME    "OFRUISVRDSDEL"

static char dsdelete_composedname[DS_DSNAME_LEN + 2] = {0,};
static char dsdelete_dsname[DS_DSNAME_LEN + 2] = {0,};
static char dsdelete_catalog[DS_DSNAME_LEN + 2] = {0,};
static char dsdelete_volser[DS_VOLSER_LEN + 2] = {0,};
static char dsdelete_member[NVSM_MEMBER_LEN + 2] = {0,};

static int dsdelete_uncatalog = 0;
static int dsdelete_force = 0;
static int dsdelete_cataloged = 0;
static int dsdelete_ignore = 0;

static int dsdelete_volser_locked = 0;
static int dsdelete_dataset_locked = 0;
static char dsdelete_lock_name[LOCKM_LOCKNAME_SIZE + 1] = {0,};
static icf_result_t dsdelete_icf_result[1];
static int nlock;
static lock_target_t *locks = NULL;
static char vollock_name[LOCKM_LOCKNAME_SIZE+1];

static int _get_params(FBUF *rcv_buf);
static int _validate_param(char *ret_msg);

static int _init_libraries();
static int _final_libraries();

static int _check_catalog_and_volser(char *ret_msg);
static int _check_dataset(char *ret_msg);
static int _lock_volume(char *ret_msg);
static int _lock_dataset(char *ret_msg);

#define UISVRDSDEL_RC_SUCCESS   0
#define UISVRDSDEL_RC_WARNING   4
void OFRUISVRDSDEL(TPSVCINFO *tpsvcinfo)
{
    int  retval;

    FBUF *rcv_buf = NULL;
    FBUF *snd_buf = NULL;

    char s_temp[1024];
    char ret_msg[1024] = {'\0',};
    
    /* service start */
    retval = svrcom_svc_start(SERVICE_NAME);
    if (retval != SVRCOM_ERR_SUCCESS ) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_SVRCOM_FUNCTION_ERROR, SERVICE_NAME, "svrcom_svc_start", retval);
        goto _DSDEL_MAIN_ERR_TPFAIL_00;
    }

    /* get rcv buffer */
    rcv_buf = (FBUF *)(tpsvcinfo->data);
    fbprint(rcv_buf); 
    
    /* allocate snd buffer */
    snd_buf = (FBUF *)tpalloc("FIELD", NULL, 0);
    if (snd_buf == NULL) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_TMAX_TPALLOC_ERROR, SERVICE_NAME, "snd_buf", tpstrerror(tperrno));
        retval = SVRCOM_ERR_TPALLOC; goto _DSDEL_MAIN_ERR_TPFAIL_00;
    }
    
    /* initialize parameters #1 */
    memset(dsdelete_composedname, 0x00, sizeof (dsdelete_composedname));
    memset(dsdelete_dsname, 0x00, sizeof (dsdelete_dsname));
    memset(dsdelete_catalog, 0x00, sizeof (dsdelete_catalog));
    memset(dsdelete_volser, 0x00, sizeof (dsdelete_volser));
    memset(dsdelete_member, 0x00, sizeof (dsdelete_member));
   
    /* initialize parameters #2 */
    dsdelete_uncatalog = 0;
    dsdelete_force = 0;
    dsdelete_cataloged = 0;
    dsdelete_volser_locked = 0;
    dsdelete_dataset_locked = 0;
    dsdelete_ignore = 0;

    /* initialize parameters #3 */
    memset(dsdelete_lock_name, 0x00, sizeof (dsdelete_lock_name));
    memset(&dsdelete_icf_result[0], 0x00, sizeof (icf_result_t));
    nlock = 0; locks = NULL;
    memset(vollock_name, 0x00, sizeof (vollock_name));

    /* get parameters */
    retval = _get_params(rcv_buf);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_INTERNAL_FUNC_ERROR, SERVICE_NAME, "_get_params", retval);
        sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "_get_params", retval);
        goto _DSDEL_MAIN_ERR_TPFAIL_01;
    }

    /* check parameters */
    retval = _validate_param(ret_msg);
    if (retval < 0)  {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_INTERNAL_FUNC_ERROR, SERVICE_NAME, "_validate_param", retval);
        sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "_validate_param", retval);
        goto _DSDEL_MAIN_ERR_TPFAIL_01;
    }

    /* uisvr login process */
    retval = uisvr_login_process(SERVICE_NAME, rcv_buf);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_INNER_FUNCTION_ERROR, SERVICE_NAME, "uisvr_login_process", retval);
        sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "uisvr_login_process", retval);
        goto _DSDEL_MAIN_ERR_TPFAIL_01;
    }

    /* initialize libraries */
    retval = _init_libraries();
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_INTERNAL_FUNC_ERROR, SERVICE_NAME, "_init_libraries", retval);
        sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "_init_libraries", retval);
        goto _DSDEL_MAIN_ERR_TPFAIL_02;
    }

    retval = _check_catalog_and_volser(s_temp);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_INTERNAL_FUNC_ERROR, SERVICE_NAME, "_check_catalog_and_volser", retval);
        sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "_check_ds_exist", retval); strcat(ret_msg, s_temp);
        goto _DSDEL_MAIN_ERR_TPFAIL_03;
    }

    if (dsdelete_uncatalog) {
        if (retval == UISVRDSDEL_RC_WARNING) { goto _DSDEL_MAIN_ERR_TPFAIL_03; }

        if (dsdelete_member[0]) {
            sprintf(ret_msg, "%s: PDS member cannot be uncataloged\n", SERVICE_NAME);
            goto _DSDEL_MAIN_ERR_TPFAIL_03;
        }

        retval = _lock_dataset(s_temp);
        if (retval < 0) {
            OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_INTERNAL_FUNC_ERROR, SERVICE_NAME, "_lock_dataset", retval);
            sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "_lock_dataset", retval); strcat(ret_msg, s_temp);
            goto _DSDEL_MAIN_ERR_TPFAIL_03;
        }

        retval = ams_delete(dsdelete_icf_result[0].catname, dsdelete_dsname, dsdelete_icf_result[0].enttype, AMS_DELETE_UNCATALOG);
        if (retval < 0) {
            OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "ams_delete", retval);
            sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "ams_delete", retval);
            goto _DSDEL_MAIN_ERR_TPFAIL_03;
        }
    } else {
        retval = _check_dataset(s_temp);
        if (retval != UISVRDSDEL_RC_SUCCESS) {
            if (retval == UISVRDSDEL_RC_WARNING && dsdelete_cataloged && dsdelete_force) {
                strcat(ret_msg, s_temp);
                retval = _lock_dataset(s_temp);
                if (retval < 0) {
                    OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_INTERNAL_FUNC_ERROR, SERVICE_NAME, "_lock_dataset", retval);
                    sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "_lock_dataset", retval); strcat(ret_msg, s_temp);
                    goto _DSDEL_MAIN_ERR_TPFAIL_03;
                }

                retval = ams_delete(dsdelete_icf_result[0].catname, dsdelete_dsname, dsdelete_icf_result[0].enttype, AMS_DELETE_UNCATALOG);
                if (retval < 0) {
                    OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "ams_delete", retval);
                    sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "ams_delete", retval);
                    goto _DSDEL_MAIN_ERR_TPFAIL_03;
                }

                retval = UISVRDSDEL_RC_WARNING;
                goto _DSDEL_MAIN_SUCCESS;
            }
            OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_INTERNAL_FUNC_ERROR, SERVICE_NAME, "_check_dataset", retval);
            sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "_check_dataset", retval); strcat(ret_msg, s_temp);
            goto _DSDEL_MAIN_ERR_TPFAIL_03;
        }

        retval = _lock_volume(s_temp);
        if (retval < 0) {
            OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_INTERNAL_FUNC_ERROR, SERVICE_NAME, "_lock_volume", retval);
            sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "_lock_volume", retval); strcat(ret_msg, s_temp);
            goto _DSDEL_MAIN_ERR_TPFAIL_03;
        }

        retval = _lock_dataset(s_temp);
        if (retval < 0) {
            OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_INTERNAL_FUNC_ERROR, SERVICE_NAME, "_lock_dataset", retval);
            sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "_lock_dataset", retval); strcat(ret_msg, s_temp);
            goto _DSDEL_MAIN_ERR_TPFAIL_03;
        }

        if (dsdelete_member[0]) {
            if (dsdelete_cataloged) {
                /* delete a cataloged dataset member */
                retval = amsx_delete_member(dsdelete_icf_result[0].catname, dsdelete_icf_result[0].entname, dsdelete_member, AMSX_DELETE_NOLOCK);
                if (retval < 0) {
                    OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMSX_FUNCTION_ERROR, SERVICE_NAME, "amsx_delete_member", retval);
                    sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "amsx_delete_member", retval);
                    goto _DSDEL_MAIN_ERR_TPFAIL_03;
                }
            } else {
                /* delete a uncataloged dataset member */
                retval = amsx_delete_member2(dsdelete_dsname, dsdelete_volser, dsdelete_member, AMSX_DELETE_NOLOCK);
                if (retval < 0) {
                    OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMSX_FUNCTION_ERROR, SERVICE_NAME, "amsx_delete_member2", retval);
                    sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "amsx_delete_member2", retval);
                    goto _DSDEL_MAIN_ERR_TPFAIL_03;
                }
            }
        } else {
            if (dsdelete_cataloged) {
                /* delete a cataloged dataset */
                retval = ams_delete(dsdelete_icf_result[0].catname, dsdelete_dsname, dsdelete_icf_result[0].enttype, AMS_DELETE_FORCE);
                if (retval < 0) {
                    OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "ams_delete", retval);
                    sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "ams_delete", retval);
                    goto _DSDEL_MAIN_ERR_TPFAIL_03;
                }
            } else {
                /* delete a uncataloged dataset */
                retval = ams_remove_nvsm_ds(dsdelete_dsname, dsdelete_volser, AMSX_DELETE_FORCE);
                if (retval < 0) {
                    OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "ams_remove_nvsm_ds", retval);
                    sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "ams_remove_nvsm_ds", retval);
                    goto _DSDEL_MAIN_ERR_TPFAIL_03;
                }
            }
        }

    }

_DSDEL_MAIN_SUCCESS:
    /* print ds delete OK */
    OFCOM_MSG_PRINTF2(UISVR_MSG_DS_DELETE_OK, SERVICE_NAME, dsdelete_composedname);

    /* finalize libraries */
    _final_libraries();
    uisvr_logout_process();

    /* fbput ret_msg */
    if (dsdelete_uncatalog) {
        sprintf(ret_msg, "%s: Dataset Uncatalog OK. dsn=%s\n", SERVICE_NAME, dsdelete_composedname);
    } else {
        sprintf(ret_msg, "%s: Dataset Delete OK. dsn=%s\n", SERVICE_NAME, dsdelete_composedname);
    }
    svrcom_fbput(snd_buf, FB_RETMSG, ret_msg, 0);

    /* service end */
    retval = svrcom_svc_end(TPSUCCESS, 0);
    tpreturn(TPSUCCESS, 0, (char *)snd_buf, 0, TPNOFLAGS);

_DSDEL_MAIN_ERR_TPFAIL_03:
    _final_libraries();

_DSDEL_MAIN_ERR_TPFAIL_02:
    uisvr_logout_process();

_DSDEL_MAIN_ERR_TPFAIL_01:
    if( retval == SAF_ERR_NOT_AUTHORIZED )
        sprintf(ret_msg, "You are not authorized to access this resource.\n");
            
    if (snd_buf) svrcom_fbput(snd_buf, FB_RETMSG, ret_msg, 0);

_DSDEL_MAIN_ERR_TPFAIL_00:
    svrcom_svc_end(TPFAIL, retval);
    UISVR_TPRETURN_TPFAIL_CHECK_DISCONNECTED(retval, (char *)snd_buf, 0, TPNOFLAGS);
}

static int _get_params(FBUF *rcv_buf)
{
    int retval;
    char r_type = ' ', *cutpos, *cutpos2;

    /* get dsname */
    retval = svrcom_fbget(rcv_buf, FB_DSNAME, dsdelete_composedname, DS_DSNAME_LEN); 
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_SVRCOM_FUNCTION_ERROR, SERVICE_NAME, "svrcom_fbget", retval);
        return retval;
    }
    
    /* get catalog */
    retval = svrcom_fbget_opt(rcv_buf, FB_CATNAME, dsdelete_catalog, DS_DSNAME_LEN);
    if (retval < 0 && retval != SVRCOM_ERR_FBNOENT) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_SVRCOM_FUNCTION_ERROR, SERVICE_NAME, "svrcom_fbget_opt", retval);
        return retval;
    }   

    /* get volume */
    retval = svrcom_fbget_opt(rcv_buf, FB_VOLUME, dsdelete_volser, DS_VOLSER_LEN);
    if (retval < 0 && retval != SVRCOM_ERR_FBNOENT) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_SVRCOM_FUNCTION_ERROR, SERVICE_NAME, "svrcom_fbget_opt", retval);
        return retval;
    }   

    /* fbget multiple options from FB_TYPE */
    while(1)
    {
        retval = svrcom_fbget_opt(rcv_buf, FB_TYPE, & r_type, 1);
        if (retval < 0 )
        {
            if (retval != SVRCOM_ERR_FBNOENT)
            {
                OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_SVRCOM_FUNCTION_ERROR, SERVICE_NAME, "svrcom_fbget", retval);
                return retval;
            }
            break;
        }
        if (r_type == 'U' || r_type == 'u')
            dsdelete_uncatalog = 1;
        else if (r_type == 'F' || r_type == 'f') 
            dsdelete_force = 1;
        else if (r_type == 'I') 
            dsdelete_ignore = 1;
    }
    
    /* compose dataset name */
    strcpy(dsdelete_dsname, dsdelete_composedname);

    /* separate member name */
    cutpos = strchr(dsdelete_dsname, '(');
    cutpos2 = strchr(dsdelete_dsname, ')');
    if ((cutpos != NULL) && (cutpos2 != NULL)) {
        *cutpos = *cutpos2 = '\0';
        strcpy(dsdelete_member, cutpos + 1);
    } 

    return 0;
}

static int _validate_param(char *ret_msg)
{
    /* check if dataset name is specified */
    if (dsdelete_dsname[0] != '\0') {
        if (dscom_check_dsname(dsdelete_dsname) < 0) {
            OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_INVALID_DSN_ERROR, SERVICE_NAME, dsdelete_dsname);
            sprintf(ret_msg, "%s: invalid dataset name - dsname=%s\n", SERVICE_NAME, dsdelete_dsname);
            if(!dsdelete_ignore)
                return SVRCOM_ERR_INVALID_PARAM;
        }
    }

    /* check if catalog name is specified */
    if (dsdelete_catalog[0] != '\0') {
        if (dscom_check_dsname(dsdelete_catalog) < 0) {
            OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_INVALID_CATNAME_ERROR, SERVICE_NAME, dsdelete_catalog);
            sprintf(ret_msg, "%s: invalid catalog name - catalog=%s\n", SERVICE_NAME, dsdelete_catalog);
            if(!dsdelete_ignore)
                return SVRCOM_ERR_INVALID_PARAM;
        }
    }

    /* check if volume serial is specified */
    if (dsdelete_volser[0] != '\0') {
        if (dscom_check_volser(dsdelete_volser) < 0) {
            OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_INVALID_VOL_ERROR, SERVICE_NAME, dsdelete_volser);
            sprintf(ret_msg, "%s: invalid volume serial - volser=%s\n", SERVICE_NAME, dsdelete_volser);
            if(!dsdelete_ignore)
                return SVRCOM_ERR_INVALID_PARAM;
        }
    }

    /* check if member name is specified */
    if (dsdelete_member[0] != '\0') {
        if (dscom_check_dsname2(dsdelete_dsname, dsdelete_member) < 0) {
            OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_INVALID_MEMBER_ERROR, SERVICE_NAME, dsdelete_member);
            sprintf(ret_msg, "%s: invalid dataset name or member name - dsname=%s,member=%s\n", SERVICE_NAME, dsdelete_dsname, dsdelete_member);
            if(!dsdelete_ignore)
                return SVRCOM_ERR_INVALID_PARAM;
        }
    }

    return 0;
}

static int _init_libraries()
{
    int retval;

    retval = amsx_initialize();
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMSX_FUNCTION_ERROR, SERVICE_NAME, "amsx_initialize", retval);
        return retval;
    }

    return 0;
}

static int _final_libraries()
{
    int i;
    pid_t lock_owner = getpid();
    if (dsdelete_volser_locked) {
        (void) lockm_unlock(vollock_name, lock_owner, LOCK_DATASET_EX);
    }
    if (dsdelete_dataset_locked) {
        for (i = 0; i < nlock; i++) {
            (void) lockm_unlock(locks[i].lock_name, lock_owner, locks[i].lock_type);
        }
    }

    (void) amsx_finalize();

    return 0;
}

static int _check_catalog_and_volser(char *ret_msg)
{
    int retval, search_flags, rcount = 1;
    ams_info_nvsm_t info;

    retval = ams_use_catalog(dsdelete_catalog);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "ams_use_catalog", retval);
        sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "ams_use_catalog", retval);
        return -1;
    }

    if (dsdelete_catalog[0]) { search_flags = AMS_SEARCH_1_CATALOG; }
    else { search_flags = AMS_SEARCH_DEFAULT; }

    retval = ams_search_entries(dsdelete_dsname, "AH", &rcount, dsdelete_icf_result, search_flags);
    if (retval < 0 && retval != AMS_ERR_NOT_FOUND) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "ams_search_entreis", retval);
        sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "ams_search_entries", retval);
        return -1;
    }

    if (retval == AMS_ERR_NOT_FOUND) {
        dsdelete_cataloged = 0;
        if (!dsdelete_volser[0]) {
            retval = volm_get_default_volume(dsdelete_volser);
            if (retval < 0) {
                OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_VOLM_FUNCTION_ERROR, SERVICE_NAME, "volm_get_default_volume", retval);
                sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "volm_get_default_volume", retval);
                return -1;
            }
        }

        if (dsdelete_catalog[0]) {
            sprintf(ret_msg, "%s: dataset is not found in the catalog - dsname=%s,catalog=%s\n", SERVICE_NAME, dsdelete_dsname, dsdelete_catalog);
            return UISVRDSDEL_RC_WARNING;
        } else if (dsdelete_uncatalog) {
            sprintf(ret_msg, "%s: dataset is not found cataloged while uncatalog option is specified - dsname=%s\n", SERVICE_NAME, dsdelete_dsname);
            return UISVRDSDEL_RC_WARNING;
        }
    } else {
        retval = ams_info(dsdelete_icf_result[0].catname, dsdelete_dsname, dsdelete_icf_result[0].enttype, &info, AMS_INFO_DEFAULT);
        if (retval < 0) {
            OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "ams_info", retval);
            sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "ams_info", retval);
            return -1;
        }

        if (dsdelete_volser[0]) {
            if (strcmp(dsdelete_volser, info.volser)) {
                dsdelete_cataloged = 0;
                if (dsdelete_uncatalog) {
                    sprintf(ret_msg, "%s: dataset is not found cataloged while uncatalog option is specified - dsname=%s,volser=%s\n", SERVICE_NAME, dsdelete_dsname, dsdelete_volser);
                    return UISVRDSDEL_RC_WARNING;
                }
            } else { dsdelete_cataloged = 1; }
        } else {
            dsdelete_cataloged = 1;
            strcpy(dsdelete_volser, info.volser);
        }
    }

    return UISVRDSDEL_RC_SUCCESS;
}

static int _check_dataset(char *ret_msg)
{
    int retval, bexist;

    retval = volm_check_ds_existency(dsdelete_volser, dsdelete_dsname, -1, &bexist);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_VOLM_FUNCTION_ERROR, SERVICE_NAME, "volm_check_ds_existency", retval);
        sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "volm_check_ds_existency", retval);
        return -1;
    }

    if (!bexist) {
        sprintf(ret_msg, "%s: dataset is not found in the volume. dsname=%s,volser=%s\n", SERVICE_NAME, dsdelete_dsname, dsdelete_volser);
        return UISVRDSDEL_RC_WARNING;
    }

    return UISVRDSDEL_RC_SUCCESS;
}

static int _lock_volume(char *ret_msg)
{
    int retval; volm_volume_t vinfo;
    char buffer[1024] = {0,};

    /* don't need to lock if already locked */
    if (dsdelete_volser_locked) { return 0; }

    /* don't need to lock DA volume */
    retval = volm_get_volume_info(dsdelete_volser, &vinfo);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_VOLM_FUNCTION_ERROR, SERVICE_NAME, "volm_get_volume_info", retval);
        sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "volm_get_volume_info", retval);
        return retval;
    }
    if (!(vinfo.devattr & VOLM_DEVATTR_TAPE)) { return 0; }

    /* don't need to lock tape volume which does not use file sequence */
    (void) ofcom_conf_get_value("ds.conf", "DATASET_DEFAULT", "USE_TAPE_FILESEQ", buffer, sizeof (buffer));
    if (buffer[0] != 'Y' && buffer[0] != 'y') { return 0; }

    /* lock the volume */
    strcpy(vollock_name, "_UNCATLG:SYS1.VTOC.V");
    strcat(vollock_name, dsdelete_volser);
    retval = lockm_lock(vollock_name, getpid(), LOCK_DATASET_EX, LOCKM_LOCK_WAIT_IMMEDIATE);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_LOCKM_FUNCTION_ERROR, SERVICE_NAME, "lockm_lock", retval);
        sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "lockm_lock", retval);
        return retval;
    }

    dsdelete_volser_locked = 1;

    return 0;
}


static int _lock_dataset(char *ret_msg)
{
    int retval;

    if (dsdelete_dataset_locked) { return 0; }

    retval = dsalc_lock_init();
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_DSALC_FUNCTION_ERROR, SERVICE_NAME, "dsalc_lock_init", retval);
        sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "dsalc_lock_init", retval);
        return retval;
    }

    strcat(dsdelete_lock_name, dsdelete_dsname);
    if (dsdelete_cataloged) {
        retval = dsalc_lock_add(dsdelete_lock_name, NULL, DSALC_LOCK_ADD_DEFAULT);
    } else {
        retval = dsalc_lock_add(dsdelete_lock_name, NULL, DSALC_LOCK_ADD_TEMPORARY);
    }
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_DSALC_FUNCTION_ERROR, SERVICE_NAME, "dsalc_lock_add", retval);
        sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "dsalc_lock_add", retval);
        return retval;
    }

    retval = dsalc_lock_list(&nlock, &locks);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_DSALC_FUNCTION_ERROR, SERVICE_NAME, "dsalc_lock_list", retval);
        sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "dsalc_lock_list", retval);
        return retval;
    }

    retval = lockm_lock_all(getpid(), nlock, locks, LOCKM_LOCK_WAIT_IMMEDIATE, NULL);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_LOCKM_FUNCTION_ERROR, SERVICE_NAME, "lockm_lock_all", retval);
        sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "lockm_lock_all", retval);
        return retval;
    }

    dsdelete_dataset_locked = 1;

    return 0;
}
