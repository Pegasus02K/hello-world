#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <usrinc/atmi.h>
#include <usrinc/fbuf.h>

#include "uisvr.h"
#include "ofcom.h"
#include "svrcom.h" 
#include "lockm.h"
#include "dscom.h"
#include "ams.h"


#include "oframe_fdl.h"
#include "msgcode_uisvr.h"

/*
 * Service Name:
 *      OFRUISVRGDGDEL
 *
 * Description:
 *      Delete an existing GDG
 *
 * Input:
 *      FB_TACF_TOKEN(string): TACF token
 *      FB_DSNAME(string): GDG name
 *      FB_CATNAME(string): catalog name
 *      FB_TYPE(string): delete option for Forced (optional)
 *
 * Output:
 *      FB_RETMSG(string): error message
 */

#define SERVICE_NAME    "OFRUISVRGDGDEL"

void OFRUISVRGDGDEL(TPSVCINFO *tpsvcinfo)
{
    int  retval;
    FBUF *rcv_buf = NULL;
    FBUF *snd_buf = NULL;
    char ret_msg[1024] = {'\0',};
    
    char s_dsname[DS_DSNAME_LEN + 2];
    char s_catalog[DS_DSNAME_LEN + 2];
 
    int search_flags = AMS_SEARCH_DEFAULT;
    int rcount = 1;
    icf_result_t result;

    char lockname[256];
	
	// added to handle Force option
	char s_force = 0;
	
	int delete_flags = AMS_DELETE_DEFAULT;

    /* initialize */
    memset(s_dsname , 0x00, sizeof(s_dsname));
    memset(s_catalog, 0x00, sizeof(s_catalog));
    memset(lockname , 0x00, sizeof(lockname));

    /* service start */
    retval = svrcom_svc_start(SERVICE_NAME);
    if (retval != SVRCOM_ERR_SUCCESS) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_SVRCOM_FUNCTION_ERROR, SERVICE_NAME, "svrcom_svc_start", retval);
        goto _GDGDEL_MAIN_ERR_TPFAIL_00;
    }

    /* get rcv buffer */
    rcv_buf = (FBUF *)(tpsvcinfo->data);
    fbprint(rcv_buf);
    
    /* allocate snd buffer */
    snd_buf = (FBUF *)tpalloc("FIELD", NULL, 0);
    if (snd_buf == NULL){
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_TMAX_TPALLOC_ERROR, SERVICE_NAME, "snd_buf", tpstrerror(tperrno));
        retval = SVRCOM_ERR_TPALLOC; goto _GDGDEL_MAIN_ERR_TPFAIL_00;
    }
    
    /* get dsname */
    retval = svrcom_fbget(rcv_buf, FB_DSNAME, s_dsname, DS_DSNAME_LEN);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_SVRCOM_FUNCTION_ERROR, SERVICE_NAME, "svrcom_fbget", retval);
        sprintf(ret_msg, UISVR_MSG_SVRCOM_FUNCTION_ERROR, SERVICE_NAME, "svrcom_fbget", retval);
        goto _GDGDEL_MAIN_ERR_TPFAIL_01;
    }
    
    /* get catname */
    retval = svrcom_fbget(rcv_buf, FB_CATNAME, s_catalog, DS_DSNAME_LEN);     
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_SVRCOM_FUNCTION_ERROR, SERVICE_NAME, "svrcom_fbget", retval);
        sprintf(ret_msg, UISVR_MSG_SVRCOM_FUNCTION_ERROR, SERVICE_NAME, "svrcom_fbget", retval);
        goto _GDGDEL_MAIN_ERR_TPFAIL_01;
    }
	
	/* get type */
	retval = svrcom_fbget_opt(rcv_buf, FB_TYPE, &s_force, 1);
	if (retval < 0 && retval != SVRCOM_ERR_FBNOENT)
	{
		OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_SVRCOM_FUNCTION_ERROR, SERVICE_NAME, "svrcom_fbget", retval);
		return retval;
	}

   	/* check if GDG name is invalid */
	if (dscom_check_dsname(s_dsname) < 0 ) {
       	OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_INVALID_DSN_ERROR, SERVICE_NAME, s_dsname);
        sprintf(ret_msg, "%s: invalid GDG name. gdgname=%s\n", SERVICE_NAME, s_dsname);
        retval = SVRCOM_ERR_INVALID_PARAM; goto _GDGDEL_MAIN_ERR_TPFAIL_01;
	}

	/* check catalog name */
	if (s_catalog[0] != '\0') {
		retval = dscom_check_dsname(s_catalog);
		if (retval < 0) {
			OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_INVALID_CATNAME_ERROR, SERVICE_NAME, s_catalog);
			sprintf(ret_msg, "%s: invalid catalog name. catname=%s\n", SERVICE_NAME, s_catalog);
			retval = SVRCOM_ERR_INVALID_PARAM; goto _GDGDEL_MAIN_ERR_TPFAIL_01;
		}
	}

	/* uisvr login process */
    retval = uisvr_login_process(SERVICE_NAME, rcv_buf);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_INNER_FUNCTION_ERROR, SERVICE_NAME, "uisvr_login_process", retval);
        sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "uisvr_login_process", retval);
        goto _GDGDEL_MAIN_ERR_TPFAIL_01;
    }

    /* initialize libraries */
    retval = ams_initialize();
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "ams_initialize", retval);
        sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "ams_initialize", retval);
        goto _GDGDEL_MAIN_ERR_TPFAIL_02;
    }

    /* use catalog before catalog search */
    retval = ams_use_catalog(s_catalog);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "ams_use_ctalog", retval);
        sprintf(ret_msg, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "ams_use_ctalog", retval);
        goto _GDGDEL_MAIN_ERR_TPFAIL_03;
    }

    if (s_catalog[0]) search_flags = AMS_SEARCH_1_CATALOG;

    /* search catalog for GDG entry */
    retval = ams_search_entries(s_dsname, "B", & rcount, & result, search_flags);
    if (retval < 0 && retval != AMS_ERR_NOT_FOUND) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "ams_search_entries", retval);
        sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "ams_search_entries", retval);
        goto _GDGDEL_MAIN_ERR_TPFAIL_03;
    }

    /* check if GDG is not found */
    if (retval == AMS_ERR_NOT_FOUND) {
        sprintf(ret_msg, "%s: GDG(%s) is not found\n", SERVICE_NAME, s_dsname);
        goto _GDGDEL_MAIN_ERR_TPFAIL_03;
    }

// added to handle Force option
	// set delete flags 
	if(s_force) delete_flags |= AMS_DELETE_FORCE;
	
    /* compose lock name */
    strcpy( lockname, result.catname );
    strcat( lockname, ":" );
    strcat( lockname, result.entname );

    /* lock GDG base */
    retval = lockm_lock(lockname, getpid(), LOCK_DATASET_EX, LOCKM_LOCK_WAIT_IMMEDIATE);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_LOCKM_FUNCTION_ERROR, SERVICE_NAME, "lockm_lock", retval);
        sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "lockm_lock", retval);
        goto _GDGDEL_MAIN_ERR_TPFAIL_03;
    }

    /* delete GDG entry */
    retval = ams_delete(result.catname, result.entname, result.enttype, delete_flags);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "ams_delete", retval);
        sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "ams_delete", retval);
        goto _GDGDEL_MAIN_ERR_TPFAIL_04;
    }

    /* unlock GDG base */
    lockm_unlock(lockname, getpid(), LOCK_DATASET_EX);

    /* print gdg delete OK */    
    OFCOM_MSG_PRINTF2(UISVR_MSG_GDG_DELETE_OK, SERVICE_NAME, s_dsname);

    /* finalize libraries */
    ams_finalize();
    uisvr_logout_process();

    /* fbput ret_msg */
    sprintf(ret_msg, "%s: GDG Delete OK. name=%s\n", SERVICE_NAME, s_dsname);
    svrcom_fbput(snd_buf, FB_RETMSG, ret_msg, 0);

    /* service end */
    svrcom_svc_end(TPSUCCESS, 0);
    tpreturn(TPSUCCESS, 0, (char *)snd_buf, 0 , TPNOFLAGS);

_GDGDEL_MAIN_ERR_TPFAIL_04:
    lockm_unlock(lockname, getpid(), LOCK_DATASET_EX);

_GDGDEL_MAIN_ERR_TPFAIL_03:
    ams_finalize();

_GDGDEL_MAIN_ERR_TPFAIL_02:
    uisvr_logout_process();

_GDGDEL_MAIN_ERR_TPFAIL_01:
    if (snd_buf) svrcom_fbput(snd_buf, FB_RETMSG, ret_msg, 0);

_GDGDEL_MAIN_ERR_TPFAIL_00:
    svrcom_svc_end(TPFAIL, retval);
    UISVR_TPRETURN_TPFAIL_CHECK_DISCONNECTED(retval, (char *)snd_buf, 0, TPNOFLAGS);
}
