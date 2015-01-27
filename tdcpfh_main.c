#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "tdcpfh.h"
#include "tcfh.h"

// translate file_status of cfile to return_code dcpfile
int tdcpfh_set_return_code(char *return_code, const char *file_status);
// called before dsio_batch_open, close, read2, write to wrap dcp file to c file
tcfh_file_t tdcpfh_dcpfile_to_cfile(const tdcpcxcb_t *dcpfile);
// called after dsio_batch_open, close, read2, write to store data from c file to dcp file
int tdcpfh_cfile_to_dcpfile(tdcpcxcb_t *dcpfile, tcfh_file_t cfile);


int OPENCXD(tdcpcxcb_t *file, char *open_mode)
{
	tcfh_file_t cfile;
	
	// translate char* open_mode to int open_mode, cannot use strcmp as open_mode not being '\0' ended string
	//01  DCPENV.
	//	03  OUT			PIC X(03)  VALUE  'OUT'.
	//	03  INP			PIC X(02)  VALUE  'IN'.
	if(open_mode[0] == 'I' && open_mode[1] == 'N') 
		file->open_mode = TCFH_OPEN_INPUT;
	else if (open_mode[0] == 'O' && open_mode[1] == 'U' && open_mode[0] == 'T') 
		file->open_mode = TCFH_OPEN_OUTPUT;
	else
	{
		fprintf(stderr, "TDCPFH: for DCP format, only \"IN\" and \"OUT\" are supported for the open mode\n");
		// should the file->return_code be changed here?
		return -1;
	}
	
	cfile = tdcpfh_dcpfile_to_cfile(file);
	tcfh_open(&cfile, cfile.open_mode, 0x00);
	
	return tdcpfh_cfile_to_dcpfile(file, cfile);
}


int CLOSECX(tdcpcxcb_t *file)
{
	tcfh_file_t cfile;
	
	cfile = tdcpfh_dcpfile_to_cfile(file);
	tcfh_close(&cfile, 0x00);
	
	return tdcpfh_cfile_to_dcpfile(file, cfile);
}


int READXD(tdcpcxcb_t *file, char *buf, char *return_code)
{
	tcfh_file_t cfile;
	
	cfile = tdcpfh_dcpfile_to_cfile(file);
	tcfh_read(&cfile, NULL, 0, buf, file->lrecl, 0x00);
	
	tdcpfh_set_return_code(return_code, cfile.file_status);
	
	return tdcpfh_cfile_to_dcpfile(file, cfile);
}


int CWRITED(tdcpcxcb_t *file, char *buf)
{
	tcfh_file_t cfile;
	
	cfile = tdcpfh_dcpfile_to_cfile(file);
	tcfh_write(&cfile, NULL, 0, buf, file->lrecl, 0x00);
	
	return tdcpfh_cfile_to_dcpfile(file, cfile);
}


int tdcpfh_set_return_code(char *return_code, const char *file_status)
{
    if (file_status[0] == '0' && file_status[1] == '0')
	{ // success
		return_code[0] = ' '; 
		return_code[1] = ' ';
		return_code[2] = ' ';
		return_code[3] = ' ';
		
		return 0;
	}
	else if (file_status[0] == '1' && file_status[1] == '0')
	{ // EOF
		return_code[0] = 'E';
		return_code[1] = 'O';
		return_code[2] = 'F'; 
		return_code[3] = ' ';
		
		return 0;
	}
	else
	{ // error 
		return_code[0] = 'E';
		return_code[1] = 'R';
		return_code[2] = 'R'; 
		return_code[3] = ' ';
	}
	
	return -1;
}


tcfh_file_t tdcpfh_dcpfile_to_cfile(const tdcpcxcb_t *dcpfile)
{
	tcfh_file_t cfile;
	
	memset(cfile.file_name, 0x00, sizeof(cfile.file_name));  
	memcpy(cfile.file_name, dcpfile->file_name, sizeof(dcpfile->file_name));
	// as file_status is return code, it's meaningless in initialization step
	cfile.file_status[0] = 0;
	cfile.file_status[1] = 0;
	cfile.organization = TCFH_ORG_SEQUENTIAL;
	cfile.access_mode = TCFH_ACCESS_SEQUENTIAL;
	cfile.open_mode = dcpfile->open_mode;
	cfile.misc_flags = 0x00;
	strcpy(cfile.file_path,"DUMMY");
	cfile.file_handle = dcpfile->file_handle;
	cfile.relative_key = 0;
	cfile.key_length = 0;
	cfile.key_loc = 0;
	cfile.rec_size = dcpfile->lrecl;
	cfile.cur_reclen = 0;
	cfile.key_address = NULL;
	cfile.record_addr = NULL;
	cfile.is_open = dcpfile->is_open;
	cfile.is_read = dcpfile->is_read;
	
	return cfile;
}


int tdcpfh_cfile_to_dcpfile(tdcpcxcb_t *dcpfile, tcfh_file_t cfile)
{
	// copy 8 bytes from cfile.file_name to dcpfile->file_name not considering '\0'
	memcpy(dcpfile->file_name, cfile.file_name, sizeof(dcpfile->file_name));
	dcpfile->lrecl = cfile.rec_size;
	dcpfile->chkpt = 'Y';
	dcpfile->monoc = 'Y';
	dcpfile->ioabend = 'Y';
	dcpfile->lrecfm = 'F';
	dcpfile->file_handle = cfile.file_handle;
	dcpfile->open_mode = cfile.open_mode;
	dcpfile->is_open = cfile.is_open;
	dcpfile->is_read = cfile.is_read;
	
	return tdcpfh_set_return_code(dcpfile->return_code, cfile.file_status);
}
