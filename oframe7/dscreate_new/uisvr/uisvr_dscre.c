#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <usrinc/atmi.h>
#include <usrinc/fbuf.h>

#include "uisvr.h"
#include "ofcom.h"
#include "svrcom.h"
#include "dscom.h"
#include "ams.h"
#include "dsalc.h"
#include "dsio_batch.h"
#include "volm.h"

#include "oframe_fdl.h"
#include "msgcode_uisvr.h"

/*
 * Service Name:
 *      OFRUISVRDSCRE
 *
 * Description:
 *      Create a new dataset.
 *
 * Input:
 *      FB_TACF_TOKEN(string): TACF token 
 *      FB_DSNAME(string): dataset name
 *      FB_ARGS(string): dataset attributes
 *      FB_TYPE(char): create options (optional)
 *
 * Format(FB_ARGS):
 *      member;volume;dsorg;recfm;unit;lrecl;blksize;expire;
 *      usercat;keylen;keypos;nocatalog;primary;secondary;avgval;
 *      directory;
 *
 * Format(FB_TYPE):
 *      R: define only the catalog entry for the dataset (recatalog)
 *      N: do not catalog the dataset newly created (nocatalog)
 *
 * Output:
 *      FB_RETMSG(strong): error message
 */

#define SERVICE_NAME    "OFRUISVRDSCRE"

extern int dscreate_chg_owner;
extern int dscreate_use_pdse_sharing;
extern int dscreate_chk_dup_catlg;
extern int dscreate_chk_dup_member;

void OFRUISVRDSCRE(TPSVCINFO *tpsvcinfo)
{
    int  retval;
    FBUF *rcv_buf = NULL;
    FBUF *snd_buf = NULL;
    char ret_msg[1024] = {'\0',};
    
    char *cutpos, s_dsname[DS_DSNAME_LEN + 2], s_buf[1023 + 1], s_temp[1024];
    char *cut_member, *cut_volume, *cut_dsorg, *cut_recfm;
    char *cut_unit, *cut_lrecl, *cut_blksize, *cut_expire;
    char *cut_usercat, *cut_keylen, *cut_keypos;
    char *cut_nocatalog, *cut_primary, *cut_secondary, *cut_avgval; 
    char *cut_directory;
   
    char r_type;
    char r_token[SAFX_TOKEN_SIZE];
    char s_catalog[100] = {0,};
    char s_composed[256];

    char filepath[1024];
    char userid[8 + 2];
    char passwd[31 + 1];

    dsalc_req_t req;
    int  handle;
	int  dsn_type;
    int  dsalc_alloc_flag = DSALC_ALLOCATE_DEFAULT;
    int  ams_search_flag = AMS_SEARCH_DEFAULT;

    dsio_dcb_t **dcbs = NULL;
    int dcb_type;	
	
    /* service start */
    retval = svrcom_svc_start(SERVICE_NAME);
    if (retval != SVRCOM_ERR_SUCCESS ) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_SVRCOM_FUNCTION_ERROR, SERVICE_NAME, "svrcom_svc_start", retval);
        goto _DSCRE_MAIN_ERR_TPFAIL_00;
    }
    
    /* get rcv buffer */
    rcv_buf = (FBUF *)(tpsvcinfo->data);
    fbprint(rcv_buf); 
    
    /* allocate snd buffer */
    snd_buf = (FBUF *)tpalloc("FIELD", NULL, 0);
    if (snd_buf == NULL) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_TMAX_TPALLOC_ERROR, SERVICE_NAME, "snd_buf", tpstrerror(tperrno));
        retval = SVRCOM_ERR_TPALLOC; goto _DSCRE_MAIN_ERR_TPFAIL_00;
    }
    
    /* get dsname */
    retval = svrcom_fbget(rcv_buf, FB_DSNAME, s_dsname, DS_DSNAME_LEN);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_SVRCOM_FUNCTION_ERROR, SERVICE_NAME, "svrcom_fbget", retval);
        sprintf(ret_msg, UISVR_MSG_SVRCOM_FUNCTION_ERROR, SERVICE_NAME, "svrcom_fbget", retval);
        goto _DSCRE_MAIN_ERR_TPFAIL_01;
    }
    
    /* get args */
    retval = svrcom_fbget(rcv_buf, FB_ARGS, s_buf, sizeof(s_buf) - 1);
    if (retval < 0) { 
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_SVRCOM_FUNCTION_ERROR, SERVICE_NAME, "svrcom_fbget", retval);
        sprintf(ret_msg, UISVR_MSG_SVRCOM_FUNCTION_ERROR, SERVICE_NAME, "svrcom_fbget", retval);
        goto _DSCRE_MAIN_ERR_TPFAIL_01;
    }

    /* get type */
    retval = svrcom_fbget_opt(rcv_buf, FB_TYPE, & r_type, 1);
    if (retval < 0 && retval != SVRCOM_ERR_FBNOENT) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_SVRCOM_FUNCTION_ERROR, SERVICE_NAME, "svrcom_fbget", retval);
        sprintf(ret_msg, UISVR_MSG_SVRCOM_FUNCTION_ERROR, SERVICE_NAME, "svrcom_fbget", retval);
        goto _DSCRE_MAIN_ERR_TPFAIL_01;
    } else if (retval == SVRCOM_ERR_FBNOENT) {
        r_type = ' ';
    }
    
    /* check dscreate_chg_owner */
    if (dscreate_chg_owner) {
        /* get TACF token */
        retval = fbget_tu(rcv_buf, FB_TACF_TOKEN, 0, r_token, 0);
        if (retval < 0) {
            OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_TMAX_FBGET_ERROR, SERVICE_NAME, "FB_TACF_TOKEN", fbstrerror(fberror));
            sprintf(ret_msg, "%s: fbget(%s) failed. err=%s\n", SERVICE_NAME, "FB_TACF_TOKEN", fbstrerror(fberror));
            retval = SVRCOM_ERR_FBGET; goto _DSCRE_MAIN_ERR_TPFAIL_01;
        }
    }
    
    /* parse input args */
    cut_member = s_buf;           cutpos = strchr(cut_member,';');      *cutpos = 0;
    cut_volume = cutpos + 1;      cutpos = strchr(cut_volume,';');      *cutpos = 0;
    cut_dsorg = cutpos + 1;       cutpos = strchr(cut_dsorg,';');       *cutpos = 0;
    cut_recfm = cutpos + 1;       cutpos = strchr(cut_recfm,';');       *cutpos = 0;
    cut_unit = cutpos + 1;        cutpos = strchr(cut_unit,';');        *cutpos = 0; 
    cut_lrecl = cutpos + 1;       cutpos = strchr(cut_lrecl,';');       *cutpos = 0; 
    cut_blksize = cutpos + 1;     cutpos = strchr(cut_blksize,';');     *cutpos = 0;
    cut_expire = cutpos + 1;      cutpos = strchr(cut_expire,';');      *cutpos = 0;
    cut_usercat = cutpos + 1;     cutpos = strchr(cut_usercat,';');     *cutpos = 0;
    cut_keylen = cutpos + 1;      cutpos = strchr(cut_keylen,';');      *cutpos = 0;
    cut_keypos = cutpos + 1;      cutpos = strchr(cut_keypos,';');      if (cutpos) *cutpos = 0;

    /* parse optional args */
    cut_nocatalog = cut_primary = cut_secondary = cut_avgval = "";
    if ( *(cutpos + 1) ) { cut_nocatalog = cutpos + 1;   cutpos = strchr(cut_nocatalog,';');   if (cutpos) *cutpos = 0; }
    if ( *(cutpos + 1) ) { cut_primary = cutpos + 1;     cutpos = strchr(cut_primary,';');     if (cutpos) *cutpos = 0; }
    if ( *(cutpos + 1) ) { cut_secondary = cutpos + 1;   cutpos = strchr(cut_secondary,';');   if (cutpos) *cutpos = 0; }
    if ( *(cutpos + 1) ) { cut_avgval = cutpos + 1;   	 cutpos = strchr(cut_avgval,';');      if (cutpos) *cutpos = 0; }

    if ( *(cutpos + 1) ) { cut_directory = cutpos + 1;   	 cutpos = strchr(cut_directory,';');      if (cutpos) *cutpos = 0; }

    /* check nocatalog option */
    if( cut_nocatalog && cut_nocatalog[0] ) {
    	if( !strcasecmp(cut_nocatalog, "YES") || (strlen(cut_nocatalog) == 1 && toupper(cut_nocatalog[0]) == 'Y') ) {
    		r_type = 'N';
    	} else {
	        OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_INVALID_NOCAT_ERROR, SERVICE_NAME, cut_nocatalog);
        	sprintf(ret_msg, "%s: invalid nocatalog value. only YES or Y is allowed. nocatalog=%s\n", SERVICE_NAME, cut_nocatalog);
        	retval = SVRCOM_ERR_INVALID_PARAM; goto _DSCRE_MAIN_ERR_TPFAIL_01;
    	}
    }

    /* check dataset name */
    dsn_type = dscom_check_dsname(s_dsname);
    if (dsn_type < 0) {
        OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_INVALID_DSN_ERROR, SERVICE_NAME, s_dsname);
        sprintf(ret_msg, "%s: invalid dataset name. dsname=%s\n", SERVICE_NAME, s_dsname);
        retval = SVRCOM_ERR_INVALID_PARAM; goto _DSCRE_MAIN_ERR_TPFAIL_01;
    }

	/* check member name */
	if ( cut_member[0] ) {
		retval = dscom_check_dsname2(s_dsname, cut_member);
		if( retval < 0 ) {
			OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_INVALID_MEMBER_ERROR, SERVICE_NAME, cut_member);
			sprintf(ret_msg, "%s: invalid dataset name or member name. dsname=%s,member=%s\n", SERVICE_NAME, s_dsname, cut_member);
			retval = SVRCOM_ERR_INVALID_PARAM; goto _DSCRE_MAIN_ERR_TPFAIL_01;
		}
	}

    /* check dsname type */
    if (dsn_type == DSCOM_DSN_TYPE_PDS_MEMBER) {
        cutpos = strchr(s_dsname,'('); *cutpos = 0;
        cut_member = cutpos + 1; /* separate member name */
        cutpos = strchr(cut_member,')'); *cutpos = 0;
        if( cut_member && cut_member[0] ) {
        	if( dscom_check_dsname2(s_dsname, cut_member) < 0 ) {
	        	OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_INVALID_MEMBER_ERROR, SERVICE_NAME, cut_member);
    	    	sprintf(ret_msg, "%s: invalid member name. member=%s\n", SERVICE_NAME, cut_member);
	        	retval = SVRCOM_ERR_INVALID_PARAM; goto _DSCRE_MAIN_ERR_TPFAIL_01;
	        }
        }
    }

    /* check if catalog name is invalid */
    if( cut_usercat && cut_usercat[0] ) { 
    	retval = dscom_check_dsname(cut_usercat);
    	if( retval < 0 ) {
        	OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_INVALID_CATNAME_ERROR, SERVICE_NAME, cut_usercat);
        	sprintf(ret_msg, "%s: invalid catalog name. catalog=%s\n", SERVICE_NAME, cut_usercat);
        	retval = SVRCOM_ERR_INVALID_PARAM; goto _DSCRE_MAIN_ERR_TPFAIL_01;
    	}	
    }
   
    /* check if volume serial is invalid */
    if( cut_volume && cut_volume[0] ) {
        if( dscom_check_volser(cut_volume) < 0 ) {
        	OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_INVALID_VOL_ERROR, SERVICE_NAME, cut_volume);
   	    	sprintf(ret_msg, "%s: invalid volume serial. volume=%s\n", SERVICE_NAME, cut_volume);
        	retval = SVRCOM_ERR_INVALID_PARAM; goto _DSCRE_MAIN_ERR_TPFAIL_01;
        }
    }    
    
    /* check if unit invalid */
    if( cut_unit && cut_unit[0] ) {
        if( dscom_check_unit(cut_unit) < 0 ) {
        	OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_INVALID_UNIT_ERROR, SERVICE_NAME, cut_unit);
   	    	sprintf(ret_msg, "%s: invalid unit name. unit=%s\n", SERVICE_NAME, cut_unit);
        	retval = SVRCOM_ERR_INVALID_PARAM; goto _DSCRE_MAIN_ERR_TPFAIL_01;
        }
    }    	
    
    /* check if dataset organization is invalid */    
    if( cut_dsorg && cut_dsorg[0] ) {
        if( dscom_check_dsorg(cut_dsorg) < 0  ) {
        	OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_INVALID_DSORG_ERROR, SERVICE_NAME, cut_dsorg);
   	    	sprintf(ret_msg, "%s: invalid dataset organization. dsorg=%s\n", SERVICE_NAME, cut_dsorg);
        	retval = SVRCOM_ERR_INVALID_PARAM; goto _DSCRE_MAIN_ERR_TPFAIL_01;
        }
    }    
        
    /* check if record format is specified */
    if( cut_recfm && cut_recfm[0] ) {
        if( dscom_check_recfm(cut_recfm) < 0  ) {
        	OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_INVALID_RECFM_ERROR, SERVICE_NAME, cut_recfm);
   	    	sprintf(ret_msg, "%s: invalid record format. recfm=%s\n", SERVICE_NAME, cut_recfm);
        	retval = SVRCOM_ERR_INVALID_PARAM; goto _DSCRE_MAIN_ERR_TPFAIL_01;
        }
    }    
    
    /* check if block size is specified */
    if( cut_blksize && cut_blksize[0] ) {
        /* check if block size is invalid */
        if( dscom_check_isdigit(cut_blksize) || dscom_check_blksize(atoi(cut_blksize)) < 0 ) {
        	OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_INVALID_BLKSIZE_ERROR, SERVICE_NAME, cut_blksize);
   	    	sprintf(ret_msg, "%s: invalid block size. blksize=%s\n", SERVICE_NAME, cut_blksize);
        	retval = SVRCOM_ERR_INVALID_PARAM; goto _DSCRE_MAIN_ERR_TPFAIL_01;
        }
    }    
        	        
    /* check if record length is invalid */
    if( cut_lrecl && cut_lrecl[0] ) {
        if( dscom_check_isdigit(cut_lrecl) || dscom_check_lrecl(atoi(cut_lrecl)) < 0 ) {
        	OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_INVALID_LRECL_ERROR, SERVICE_NAME, cut_lrecl);
   	    	sprintf(ret_msg, "%s: invalid record length. lrecl=%s\n", SERVICE_NAME, cut_lrecl);
        	retval = SVRCOM_ERR_INVALID_PARAM; goto _DSCRE_MAIN_ERR_TPFAIL_01;
        }
    }       
	
    /* check if primary is invalid */
	if( cut_primary && cut_primary[0] ) {
		if( dscom_check_isdigit(cut_primary) ) {
        	OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_INVALID_SECONDARY_ERROR, SERVICE_NAME, cut_primary);
	    	sprintf(ret_msg, "%s: invalid primary space. primary=%s\n", SERVICE_NAME, cut_primary);
      		retval = SVRCOM_ERR_INVALID_PARAM; goto _DSCRE_MAIN_ERR_TPFAIL_01;
		}
	}	
	
    /* check if secondary is invalid */
	if( cut_secondary && cut_secondary[0] ) {
		if( dscom_check_isdigit(cut_secondary) ) {
        	OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_INVALID_PRIMARY_ERROR, SERVICE_NAME, cut_secondary);
	    	sprintf(ret_msg, "%s: invalid secondary space. secondary=%s\n", SERVICE_NAME, cut_secondary);
       		retval = SVRCOM_ERR_INVALID_PARAM; goto _DSCRE_MAIN_ERR_TPFAIL_01;
		}
	}
	
    /* check if directory is invalid */
	if( cut_directory && cut_directory[0] ) {
		if( dscom_check_isdigit(cut_directory) ) {
        	OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_INVALID_DIR_ERROR, SERVICE_NAME, cut_directory);
	    	sprintf(ret_msg, "%s: invalid secondary space. secondary=%s\n", SERVICE_NAME, cut_directory);
       		retval = SVRCOM_ERR_INVALID_PARAM; goto _DSCRE_MAIN_ERR_TPFAIL_01;
		}
	}	
    
    /* check if key length is specified */
    if( cut_keylen && cut_keylen[0] && atoi(cut_keylen) ) {
        /* check if key length is invalid */
        if( dscom_check_isdigit(cut_keylen) || dscom_check_keylen(atoi(cut_keylen)) < 0 ) {
        	OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_INVALID_KEYLEN_ERROR, SERVICE_NAME, cut_keylen);
   	    	sprintf(ret_msg, "%s: invalid key length. keylen=%s\n", SERVICE_NAME, cut_keylen);
        	retval = SVRCOM_ERR_INVALID_PARAM; goto _DSCRE_MAIN_ERR_TPFAIL_01;
        }

        /* check if key offset is invalid */
        if( dscom_check_isdigit(cut_keypos) || dscom_check_keyoff(atoi(cut_keypos)) < 0 ) {
        	OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_INVALID_KEYPOS_ERROR, SERVICE_NAME, cut_keypos);
   	    	sprintf(ret_msg, "%s: invalid key position. keypos=%s\n", SERVICE_NAME, cut_keypos);
        	retval = SVRCOM_ERR_INVALID_PARAM; goto _DSCRE_MAIN_ERR_TPFAIL_01;
        }

        /* check if key length + offset exceed record length */
        if( atoi(cut_keylen) < 0 || atoi(cut_lrecl) < atoi(cut_keylen) + atoi(cut_keypos) ) {
        	OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_INVALID_KEYINFO_ERROR, SERVICE_NAME, cut_keypos);
   	    	sprintf(ret_msg, "%s: invalid key information. lrecl=%s,keylen=%s,keypos=%s\n", SERVICE_NAME, cut_lrecl, cut_keylen, cut_keypos);
        	retval = SVRCOM_ERR_INVALID_PARAM; goto _DSCRE_MAIN_ERR_TPFAIL_01;
        }

    /* otherwise key length is not specified */
    } else {
        /* check if dataset organization is IS */
        if( ! strcasecmp(cut_dsorg, "IS") ) {
        	OFCOM_MSG_FPRINTF1(stderr, UISVR_MSG_INVALID_ISAM_KEYLEN_ERROR, SERVICE_NAME);
   	    	sprintf(ret_msg, "%s: key length must be specified for ISAM dataset\n", SERVICE_NAME);
        	retval = SVRCOM_ERR_INVALID_PARAM; goto _DSCRE_MAIN_ERR_TPFAIL_01;
        }
        
        /* check if key position is specified */
        if( cut_keypos && cut_keypos[0] && atoi(cut_keypos) ) {
        	OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_INVALID_KEYPOS_ERROR, SERVICE_NAME, cut_keypos);
   	    	sprintf(ret_msg, "%s: key position must be specified with key length.\n", SERVICE_NAME);
        	retval = SVRCOM_ERR_INVALID_PARAM; goto _DSCRE_MAIN_ERR_TPFAIL_01;
        }        
    }    
    
    /* check if expiration date is specified */
    if( cut_expire && cut_expire[0] ) {
		/* check if expiration date is invalid */
		if( dscom_check_expdt_yyyymmdd(cut_expire) ) {
			OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_INVALID_EXPDATE_ERROR, SERVICE_NAME, cut_expire);
			sprintf(ret_msg, "%s: invalid expiration date. expdt=%s\n", SERVICE_NAME, cut_expire);
			retval = SVRCOM_ERR_INVALID_PARAM; goto _DSCRE_MAIN_ERR_TPFAIL_01;
		}
    }    

	/* check if avgval is specified */
	if( cut_avgval && cut_avgval[0] ) {
		/* check if avgval is invalid */
		if( dscom_check_avgval(cut_avgval) ) {
			OFCOM_MSG_FPRINTF2(stderr, UISVR_MSG_INVALID_SPACE_UNIT_ERROR, SERVICE_NAME, cut_avgval);
			sprintf(ret_msg, "%s: invalid TRK/CYL/blklth value. TRK/CYL/blklgth=%s\n", SERVICE_NAME, cut_avgval);
			retval = SVRCOM_ERR_INVALID_PARAM; goto _DSCRE_MAIN_ERR_TPFAIL_01;
		}
	}
    
    /* uisvr login process */
    retval = uisvr_login_process(SERVICE_NAME, rcv_buf);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_INNER_FUNCTION_ERROR, SERVICE_NAME, "uisvr_login_process", retval);
        sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "uisvr_login_process", retval);
        goto _DSCRE_MAIN_ERR_TPFAIL_01;
    }

    /* initialize libraries */
    retval = ams_initialize();
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "ams_initialize", retval);
        sprintf(ret_msg, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "ams_initialize", retval);
        goto _DSCRE_MAIN_ERR_TPFAIL_02;
    }

    retval = dsalc_initialize();
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_DSALC_FUNCTION_ERROR, SERVICE_NAME, "dslac_initialize", retval);
        sprintf(ret_msg, UISVR_MSG_DSALC_FUNCTION_ERROR, SERVICE_NAME, "dslac_initialize", retval);
        goto _DSCRE_MAIN_ERR_TPFAIL_03;
    }

    retval = dsio_batch_initialize(DSIO_BATCH_INIT_NVSM);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_DSIO_FUNCTION_ERROR, SERVICE_NAME, "dsio_batch_initialize", retval);
        sprintf(ret_msg, UISVR_MSG_DSIO_FUNCTION_ERROR, SERVICE_NAME, "dsio_batch_initialize", retval);
        goto _DSCRE_MAIN_ERR_TPFAIL_04;
    }
    
    /* catalog name */
    if (cut_usercat && cut_usercat[0] ) { strcpy(s_catalog, cut_usercat); ams_search_flag = AMS_SEARCH_1_CATALOG; }
    else { ams_use_catalog(NULL); ams_candidate_catalog(s_dsname, s_catalog); }    

    /* initialize allocation request */
    memset(&req, 0x00, sizeof(dsalc_req_t));

    if (cut_member && cut_member[0]) {
        if (dscreate_use_pdse_sharing)
            req.disp.status = DISP_STATUS_SHR;
        else
            req.disp.status = DISP_STATUS_OLD;
        req.disp.normal = DISP_TERM_KEEP;
        req.disp.abnormal = DISP_TERM_KEEP;
    } else if (toupper(r_type) == 'N') {
        req.disp.status = DISP_STATUS_NEW;
        req.disp.normal = DISP_TERM_KEEP;
        req.disp.abnormal = DISP_TERM_DELETE;
    } else if (toupper(r_type) == 'R') {
        req.disp.status = DISP_STATUS_OLD;
        req.disp.normal = DISP_TERM_CATLG;
        req.disp.abnormal = DISP_TERM_KEEP;
    } else {
        req.disp.status = DISP_STATUS_NEW;
        req.disp.normal = DISP_TERM_CATLG;
        req.disp.abnormal = DISP_TERM_DELETE;
    }

    strcpy(req.job.jobname, "OFRUISVR");
    strcpy(req.job.steppath, "DSCREATE");

    strcpy(req.volume.unit, cut_unit);
    strcpy(req.volume.vlist, cut_volume);

    req.space.primary = atoi(cut_primary);
    req.space.secondary = atoi(cut_secondary);

    req.space.directory = atoi(cut_directory);


    req.space.avgval = atoi(cut_avgval);
	if( !req.space.avgval ) {
		if( ! strcasecmp(cut_avgval, "CYL") )
			req.space.avgval = DS_SPACE_CYL;
		else if( ! strcasecmp(cut_avgval, "TRK") )
			req.space.avgval = DS_SPACE_TRK;
	 	else if( req.space.primary ) 
			req.space.avgval = 1024;
	}

    strcpy(req.dsattr.dsorg, cut_dsorg);
    strcpy(req.dsattr.recfm, cut_recfm);

    req.dsattr.blksize = atoi(cut_blksize);
    req.dsattr.lrecl = atoi(cut_lrecl);
    req.dsattr.keylen = atoi(cut_keylen);
    req.dsattr.keyoff = atoi(cut_keypos);

    strcpy(req.dsattr.expdt, cut_expire);

	/* set lock wait flag */
	req.lock.lock_wait = LOCKM_LOCK_WAIT_IMMEDIATE;

    /* set default catalog */
    retval = ams_use_catalog(s_catalog);
    if (retval < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "ams_use_ctalog", retval);
        sprintf(ret_msg, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "ams_use_ctalog", retval);
        goto _DSCRE_MAIN_ERR_TPFAIL_05;
    }
    
    /* dataset name */
    if (cut_member && cut_member[0]) {
        sprintf(s_composed, "%s(%s)", s_dsname, cut_member);

        /* check duplication member */
        if (dscreate_chk_dup_member) {
            char volser[6+2] = {0,};
            icf_result_t icf_result[1];
            ams_info_nvsm_t info;
            int rcount = 1, bexist;

            /* check member duplication */
            if (cut_volume && cut_volume[0]) { strcpy(volser, cut_volume); }
            else {
                retval = ams_search_entries(s_dsname, "AH", &rcount, icf_result, ams_search_flag);
                if (retval < 0 && retval != AMS_ERR_NOT_FOUND) { 
                    OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "ams_search_entries", retval);
                    sprintf(ret_msg, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "ams_search_entries", retval);
                    goto _DSCRE_MAIN_ERR_TPFAIL_05;
                }

                if (retval == AMS_ERR_SUCCESS) {
                    retval = ams_info(icf_result[0].catname, s_dsname, icf_result[0].enttype, &info, AMS_INFO_DEFAULT);
                    if (retval < 0) {
                        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "ams_info", retval);
                        sprintf(ret_msg, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "ams_info", retval);
                        goto _DSCRE_MAIN_ERR_TPFAIL_05;
                    }

                    strcpy(volser, info.volser);
                }
            }

            if (volser[0]) {
                retval = volm_check_ds_existency(volser, s_dsname, 0, &bexist);
                if (retval < 0) { 
                    OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_VOLM_FUNCTION_ERROR, SERVICE_NAME, "volm_check_ds_existency", retval);
                    sprintf(ret_msg, UISVR_MSG_VOLM_FUNCTION_ERROR, SERVICE_NAME, "volm_check_ds_existency", retval);
                    goto _DSCRE_MAIN_ERR_TPFAIL_05;
                }

                if (bexist) {
                    char temppath[DS_PATHNAME_LEN+1]; struct stat buf;

                    retval = volm_get_volume_path(volser, temppath);
                    if (retval < 0) { 
                        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_VOLM_FUNCTION_ERROR, SERVICE_NAME, "volm_get_volume_path", retval);
                        sprintf(ret_msg, UISVR_MSG_VOLM_FUNCTION_ERROR, SERVICE_NAME, "volm_get_volume_path", retval);
                        goto _DSCRE_MAIN_ERR_TPFAIL_05;
                    }

                    strcat(temppath, "/");
                    strcat(temppath, s_dsname);
                    strcat(temppath, "/");
                    strcat(temppath, cut_member);

                    errno = 0;
                    retval = lstat(temppath, &buf);
                    if (retval < 0) {
                        if (errno != ENOENT) {
                            OFCOM_MSG_FPRINTF4(stderr, UISVR_MSG_STAT_FUNC_ERROR, SERVICE_NAME, temppath, strerror(errno), retval);
                            sprintf(ret_msg, UISVR_MSG_STAT_FUNC_ERROR, SERVICE_NAME, temppath, strerror(errno), retval);
                            goto _DSCRE_MAIN_ERR_TPFAIL_05;
                        }
                    } else {
                        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_DUPLICATE_MEMBER_FOUND, SERVICE_NAME, s_dsname, cut_member);
                        sprintf(ret_msg, UISVR_MSG_DUPLICATE_MEMBER_FOUND, SERVICE_NAME, s_dsname, cut_member);
                        retval = -1; goto _DSCRE_MAIN_ERR_TPFAIL_05;
                    }
                }
            }
        }

    } else {
        strcpy(s_composed, s_dsname);
    }

    /* set allocation flag */
    if (dscreate_chk_dup_catlg) dsalc_alloc_flag |= DSALC_ALLOCATE_CHKCATLG;

    /* allocate the dataset */
    handle = dsalc_allocate("DSCREATE", s_composed, & req, dsalc_alloc_flag);
    if (handle < 0) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_DSIO_FUNCTION_ERROR, SERVICE_NAME, "dsalc_allocate", handle);
        sprintf(ret_msg, UISVR_MSG_DSIO_FUNCTION_ERROR, SERVICE_NAME, "dsalc_allocate", handle);
        retval = handle; goto _DSCRE_MAIN_ERR_TPFAIL_05;
    }

	if (toupper(r_type) != 'R') {    
        int dsio_flags = DSIO_OPEN_OUTPUT | DSIO_ACCESS_SEQUENTIAL | DSIO_LOCK_EXCLUSIVE;
        retval = dsio_batch_open(dsalc_get_dcbs(handle), dsio_flags);
        if (retval < 0) {
    	    OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_DSIO_FUNCTION_ERROR, SERVICE_NAME, "dsio_batch_open", retval);
        	sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "dsio_batch_open", retval);
        	goto _DSCRE_MAIN_ERR_TPFAIL_07;
    	}
        retval = dsio_batch_close(retval, DSIO_CLOSE_DEFAULT);
        if (retval < 0) {
            OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_DSIO_FUNCTION_ERROR, SERVICE_NAME, "dsio_batch_close", retval);
            sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "dsio_batch_close", retval);
            goto _DSCRE_MAIN_ERR_TPFAIL_07;
        }
    }
    
    /* if recatalog, update dcb information for record count. refer to IMS65988 */
	if (toupper(r_type) == 'R') {    
    	/* retrieve dcbs from allocation handle */
    	dcbs = dsalc_get_dcbs(handle);
    	if (! dcbs) {
        	retval = -1;
        	OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_DSALC_FUNCTION_ERROR, SERVICE_NAME, "dsalc_get_dcbs", retval);
        	sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "dsalc_get_dcbs", retval);
        	goto _DSCRE_MAIN_ERR_TPFAIL_07;
    	}

    	/* retrieve dcb_type & dcb_count from dcbs */
    	retval = dsio_dcb_get_type(dcbs, & dcb_type, NULL);
    	if (retval < 0) {
	        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_DSIO_FUNCTION_ERROR, SERVICE_NAME, "dsio_dcb_get_type", retval);
        	sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "dsio_dcb_get_type", retval);
        	goto _DSCRE_MAIN_ERR_TPFAIL_07;
    	}

    	/* check if dataset is non-vsam */
    	if( dcb_type == DSIO_DCB_TYPE_NVSM ) {
	        /* call a function to update DCB report */
        	retval = dsio_update_report(dcbs, 0);
        	if (retval < 0) {
	            OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_DSIO_FUNCTION_ERROR, SERVICE_NAME, "dsio_update_report", retval);
            	sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "dsio_update_report", retval);
            	goto _DSCRE_MAIN_ERR_TPFAIL_07;
        	}
    	}
    }

    /* dispose */
    retval = dsalc_dispose(handle, DISP_COND_NORMAL, DSALC_DISPOSE_DEFAULT);
    if (retval != DSALC_ERR_SUCCESS) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_DSALC_FUNCTION_ERROR, SERVICE_NAME, "dsalc_dispose", retval);
        sprintf(ret_msg, UISVR_MSG_DSALC_FUNCTION_ERROR, SERVICE_NAME, "dsalc_dispose", retval);
        goto _DSCRE_MAIN_ERR_TPFAIL_06;
    }

    /* unallocate */
    retval = dsalc_unallocate(handle, DISP_COND_NORMAL, DSALC_UNALLOCATE_DEFAULT);
    if (retval != DSALC_ERR_SUCCESS) {
        OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_DSALC_FUNCTION_ERROR, SERVICE_NAME, "dsalc_unallocate", retval);
        sprintf(ret_msg, UISVR_MSG_DSALC_FUNCTION_ERROR, SERVICE_NAME, "dsalc_unallocate", retval);
        goto _DSCRE_MAIN_ERR_TPFAIL_05;
    }

    /* print ds owner change */
    OFCOM_MSG_PRINTF2(UISVR_MSG_DS_OWNER_CHANGE, SERVICE_NAME, dscreate_chg_owner);

    /* change owner */
    if (dscreate_chg_owner) {
        retval = svrcom_saf_get_passwd(r_token, userid, passwd);
        if (retval < 0) {
            OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_SVRCOM_FUNCTION_ERROR, SERVICE_NAME, "svrcom_saf_get_passwd", retval);
            sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "svrcom_saf_get_passwd", retval);
            goto _DSCRE_MAIN_ERR_TPFAIL_05;
        }

        retval = ams_filepath(s_composed, cut_volume, filepath);
        if (retval < 0) {
            OFCOM_MSG_FPRINTF3(stderr, UISVR_MSG_AMS_FUNCTION_ERROR, SERVICE_NAME, "ams_filepath", retval);
            sprintf(ret_msg, "%s: %s() failed. rc=%d\n", SERVICE_NAME, "ams_filepath", retval);
            goto _DSCRE_MAIN_ERR_TPFAIL_05;
        }

        sprintf(s_temp, "chown %s %s", userid, filepath);
        retval = system(s_temp);
        if (retval < 0) {
            OFCOM_MSG_FPRINTF4(stderr, UISVR_MSG_SYSTEM_FUNC_ERROR, SERVICE_NAME, s_temp, strerror(errno), retval);
            sprintf(ret_msg, "%s: system(%s) failed. err=%s,rc=%d\n", SERVICE_NAME, s_temp, strerror(errno), retval);
            goto _DSCRE_MAIN_ERR_TPFAIL_05;
        }
    }
       
    /* print ds create OK */
    OFCOM_MSG_PRINTF2(UISVR_MSG_DS_CREATE_OK, SERVICE_NAME, s_composed);

    /* finalize libraries */
    dsio_batch_finalize();
    dsalc_finalize();
    ams_finalize();

    uisvr_logout_process();
    
    /* fbput ret_msg */
	if (r_type == 'R') {
		sprintf(ret_msg, "%s: Dataset Recatalog OK. dsn=%s\n", SERVICE_NAME, s_composed);
	} else {
    	sprintf(ret_msg, "%s: Dataset Create OK. dsn=%s\n", SERVICE_NAME, s_composed);
	}

    svrcom_fbput(snd_buf, FB_RETMSG, ret_msg, 0);

    /* service end */
    svrcom_svc_end(TPSUCCESS, 0);
    tpreturn(TPSUCCESS, 0, (char *)snd_buf, 0, TPNOFLAGS);

_DSCRE_MAIN_ERR_TPFAIL_07:
    dsalc_dispose(handle, DISP_COND_ABNORMAL, DSALC_DISPOSE_DEFAULT);

_DSCRE_MAIN_ERR_TPFAIL_06:
    dsalc_unallocate(handle, DISP_COND_ABNORMAL, DSALC_UNALLOCATE_DEFAULT);

_DSCRE_MAIN_ERR_TPFAIL_05:
    dsio_batch_finalize();

_DSCRE_MAIN_ERR_TPFAIL_04:
    dsalc_finalize();

_DSCRE_MAIN_ERR_TPFAIL_03:
    ams_finalize();

_DSCRE_MAIN_ERR_TPFAIL_02:
    uisvr_logout_process();

_DSCRE_MAIN_ERR_TPFAIL_01:
	if( retval == SAF_ERR_NOT_AUTHORIZED )
		sprintf(ret_msg, "You are not authorized to access this resource.\n");	
		
    if ( snd_buf ) svrcom_fbput(snd_buf, FB_RETMSG, ret_msg, 0);

_DSCRE_MAIN_ERR_TPFAIL_00:
    svrcom_svc_end(TPFAIL, retval);
    UISVR_TPRETURN_TPFAIL_CHECK_DISCONNECTED(retval, (char *)snd_buf, 0, TPNOFLAGS);
}
