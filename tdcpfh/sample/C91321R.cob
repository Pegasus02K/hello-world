       IDENTIFICATION          DIVISION.                                
       PROGRAM-ID.             C91321R.                                 
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
       01  INR-REC.                                                    
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
       01  INDD.                                                                
           03  DDNAME          PIC X(08)  VALUE   'INDD'.
           03  LRECL           PIC 9(04)  COMP-5    VALUE 0.                      
           03  CHKPT           PIC 9(04)  COMP-5    VALUE 0.                      
           03  COPYC           PIC X(01)  VALUE  'Y'.                           
           03  MONOC           PIC X(01)  VALUE  'Y'.                           
           03  IOABEND         PIC X(01)  VALUE  'Y'.                           
           03  LRECFM          PIC X(01)  VALUE  'F'.                           
           03  SYSTEM          PIC X(112).                                      
      *----------------------------------------------------------------*
      *            \B6 \B3 \DD \C4   \B4 \D8 \B1                                     *
      *----------------------------------------------------------------*
       01      ICNT                    PIC  9(008)  COMP-3  VALUE  ZERO.
       01      EOF1                    PIC  X(001)  VALUE  ZERO.
      *----------------------------------------------------------------*
       PROCEDURE               DIVISION.                                
       L0-MAIN                 SECTION.                                 
           PERFORM     L1-INIT.                                         
           PERFORM     L2-EXEC
                       UNTIL         EOF1   = '1'.
           PERFORM     L3-END.                                          
       L0-MAIN-END.                                                     
           STOP        RUN.                                             
      ******************************************************************
      *                    INIT                            1           *
      ******************************************************************
       L1-INIT                 SECTION.                                 
       L1-S.
           MOVE        40              TO      LRECL   OF  INDD.        
           CALL        'OPENCXD'       USING   INDD    INP.             
       L1-E.
           EXIT.
      ******************************************************************
      *                    EXEC    ( \B8ض\B4\BC \BC\AE\D8 )           2           *
      ******************************************************************
       L2-EXEC                 SECTION.                                 
       L2-S.
           CALL        'READXD'    USING   INDD   INR-REC     RETCD1.
           IF          RETCD1      =       'EOF '                        
               MOVE    '1'         TO      EOF1                         
               GO  TO              L2-E                                 
           END-IF.                                                      
           COMPUTE     ICNT        =       ICNT   +  1.
       L2-E.   
           EXIT.
      ******************************************************************
      *                    END                             3           *
      ******************************************************************
       L3-END                  SECTION.                                 
       L3-S.
           DISPLAY     'INDD   - COUNT :' ICNT.
           CALL        'CLOSECX'       USING   INDD.                    
       L3-E.
           EXIT.
