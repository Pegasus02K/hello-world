       IDENTIFICATION          DIVISION.                                
       PROGRAM-ID.             C91321W.                                 
      ******************************************************************
       ENVIRONMENT             DIVISION.                                
       CONFIGURATION           SECTION.                                 
      ******************************************************************
      *            DECLARE I/O FILES                                   *
      ******************************************************************
       INPUT-OUTPUT            SECTION.                                 
       FILE-CONTROL.                                                    
       DATA                    DIVISION.                                
      ******************************************************************
      *            DECLARE I/O AREAS                                   *
      ******************************************************************
       FILE                    SECTION.                                 
      ******************************************************************
      *            DECLARE WORKING AREAS                               *
      ******************************************************************
       WORKING-STORAGE         SECTION.                                 
      *----------------------------------------------------------------*
       01  OUTR-REC.                                                   
           03  RECCNT                  PIC 9(02).
           03  FILLER                  PIC X(38) VALUE   SPACE.
      *----------------------------------------------------------------*
       01  DCPENV.                                                              
           03  OUT             PIC X(03)  VALUE  'OUT'.                         
           03  INP             PIC X(02)  VALUE  'IN'.                          
           03  RETCD1          PIC X(04).                                       
           03  RETCD2          PIC X(04).                                       
           03  RETCD3          PIC X(04).                                       
           03  RETCD4          PIC X(04).                                       
           03  RETCD5          PIC X(04).                                       
           03  RETCD6          PIC X(04).                                       
           03  RETCD7          PIC X(04).                                       
           03  RETCD8          PIC X(04).                                       
           03  RETCD9          PIC X(04).                                       
       01  OUTDD.                                                               
           03  DDNAME          PIC X(08)  VALUE   'OUTDD'.                      
           03  LRECL           PIC 9(04)  COMP-5  VALUE 0.                      
           03  CHKPT           PIC 9(04)  COMP-5  VALUE 0.                      
           03  COPYC           PIC X(01)  VALUE  'Y'.                           
           03  MONOC           PIC X(01)  VALUE  'Y'.                           
           03  IOABEND         PIC X(01)  VALUE  'Y'.                           
           03  LRECFM          PIC X(01)  VALUE  'F'.                           
           03  SYSTEM          PIC X(112).                                      
      *----------------------------------------------------------------*
      *            \B6 \B3 \DD \C4   \B4 \D8 \B1                                     *
      *----------------------------------------------------------------*
       01      OCNT                    PIC  9(008)  COMP-3  VALUE  ZERO.
      *----------------------------------------------------------------*
       PROCEDURE               DIVISION.                                
       L0-MAIN                 SECTION.                                 
           PERFORM     L1-INIT.                                         
           PERFORM     L2-EXEC
                       UNTIL         OCNT   = 10.
           PERFORM     L3-END.                                          
       L0-MAIN-END.                                                     
           STOP        RUN.                                             
      ******************************************************************
      *                    INIT                            1           *
      ******************************************************************
       L1-INIT                 SECTION.                                 
           MOVE        40              TO      LRECL   OF  OUTDD.       
           CALL        'OPENCXD'       USING   OUTDD   OUT.             
      ******************************************************************
      *                    EXEC    ( \B8ض\B4\BC \BC\AE\D8 )           2           *
      ******************************************************************
       L2-EXEC                 SECTION.                                 
      *                                                                 
           COMPUTE OCNT            =       OCNT        +   1.
           MOVE    OCNT            TO      RECCNT.
           CALL    'CWRITED'       USING   OUTDD       OUTR-REC.       
      *                                                                 
      ******************************************************************
      *                    END                             3           *
      ******************************************************************
       L3-END                  SECTION.                                 
      *                                                                 
           DISPLAY     'OUTDD  - COUNT :' OCNT.
           CALL        'CLOSECX'       USING   OUTDD.                   
      *                                                                 
      *                                                                 
