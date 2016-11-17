//
//  main.cpp
//  geoimport
//
//  Created by Robin Demetriades on 26/10/2016.
//  Copyright © 2016 (c) Robin Demetriades. All rights reserved.
//
#include <libpq-fe.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <sys/stat.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <stdlib.h>
#include <errno.h>
#include <dispatch/dispatch.h>
#if defined(__linux__)
 #include <bsd/string.h>
#endif

typedef enum YESNO { NO=0,YES=1 } BOOL;
typedef enum BLOCKORLOC { IPBLOCKS=0,LOCATIONS=1 } FILETYPE;
const int PROGRAM_SUCCESS = 0;
const int PROGRAM_FAILED = -1;

#ifndef DISPATCH_QUEUE_SERIAL
 #define DISPATCH_QUEUE_SERIAL	NULL
#endif

static void ProgramCleanup(void);
static int Usage();
static int GetCommandlineOptions(int argc,
								 const char* argv[],
								 const char** strDbName,
								 const char** strFilename,
								 const char** strConnxString );
static char* ReadLine( char** ppCurPos, const char* endPos );
static const char* AdjustEndPointer(int fdInputFile, const char* pBufferStart, const char* endPos);

static uint32_t ProcessLocations(char* currentPos, const char* endPos, PGconn* PqConn, BOOL* didFail);
static uint32_t ProcessBlocks(char* currentPos, const char* endPos, PGconn* PqConn, BOOL* didFail);

static BOOL AddLocationToDatabase(const char* geoname_id, const char* continent_code, const char* city_name,
								  const char* country_iso_code, const char* country_name,
								  const char* subdivision_1_iso_code, const char* subdivision_1_name,
								  const char* subdivision_2_iso_code, const char* subdivision_2_name,
								  PGconn* PqConn);
static BOOL AddIPBlockToDatabase(const char* network,
								  const char* geoname_id,
								  const char* postal_code,
								  PGconn* PqConn);

static int InputFile = 0;

const int OneKB = 1024;
const int OneMB = OneKB * OneKB;

static off_t	FileTotalSize = 0;
static off_t	FileBytesRemaining = 0;

static uint16_t	NumProcessors = 3;

// Used to terminate the worker blocks.
static volatile BOOL AbortProgram = NO;

static BOOL ReadHeader( int fdInputFile, FILETYPE* pFileMode, uint16_t* pHeaderSize );
static off_t LoadFileBlock( char* pWriteBuffer, const char* endPos, const char** ppOutEndPos, off_t* filePos);
static uint32_t ProcessFile(FILETYPE fileMode,const char* strDbName,dispatch_queue_t loadFromFileQ,uint16_t procId);


class PostgresConnection
{
	PGconn* m_connx = NULL;
	
public:
	PostgresConnection() {}
	PostgresConnection(const PostgresConnection&) = delete;
	
	PostgresConnection(PGconn* connx) : m_connx(connx) {}
	
	operator PGconn*() const { return m_connx; }
	
	BOOL Connect(const char* strDbName);
	
	~PostgresConnection()
	{
		if( NULL!=m_connx ){
			::PQfinish(m_connx);
			m_connx = NULL;
		}
	}
};

/*
 Usage
 
 geoimport -D dbname /path/to/file.csv
*/
int main( int argc, const char* argv[] )
{
	// look at commandline options
	if(argc < 4 ){ return Usage(); }
	
	const char* strConnxString = NULL; // may be URI or connection string.
	const char* strDbName = NULL;
	const char* strFilename = NULL;
	int nStat = GetCommandlineOptions( argc-1, &argv[1], &strDbName,&strFilename,&strConnxString );
	if( 0!=nStat ){ return Usage(); }
	
	// verify the filename
	struct stat csvFileInfo = {0};
	if( 0!=stat(strFilename, &csvFileInfo) )
	{
		perror("Failed to open .csv file.");
		return 1;
	}
	
	atexit(ProgramCleanup);
	
	// Open the File
	InputFile = open(strFilename, O_RDONLY);
	if( -1==InputFile ){
		perror("Error on opening .csv file.");
		return PROGRAM_FAILED;
	}
	FileTotalSize = csvFileInfo.st_size;
	
	// Examine the header, will determine the contents of file (cities or netblocks).
	FILETYPE fileMode;
	uint16_t nHeaderSize = 0;
	if( NO==ReadHeader( InputFile, &fileMode, &nHeaderSize )){
		return PROGRAM_FAILED;
	}
	
	FileBytesRemaining = FileTotalSize - nHeaderSize;
	
	// adjust the number of processor based on input file size, if needed.
	uint64_t nBlocksToProcess = (FileBytesRemaining/OneMB);
	if( nBlocksToProcess<NumProcessors )
	{
		NumProcessors = nBlocksToProcess - 1;
	}
	if( (int16_t)NumProcessors<=0 ){ NumProcessors=1; }
	dprintf(STDOUT_FILENO,"Processing File: %lld bytes (%lld MB) using %u processing threads.\n",
		(long long)FileBytesRemaining, (long long)((FileTotalSize/OneMB)), NumProcessors);

	dispatch_group_t fileProcGrp = dispatch_group_create();
	
	// get normal priority queue
	dispatch_queue_t dpQ = dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT,0);
	
	// create the File reader sync queue
	dispatch_queue_t loadFromFileQ = dispatch_queue_create("geoimp.fileload.syncq", DISPATCH_QUEUE_SERIAL);
	
	for(uint16_t nCount=1; nCount<=NumProcessors; ++nCount )
	{
		dispatch_group_async(fileProcGrp, dpQ,
			 ^{
				 dprintf(STDOUT_FILENO,"Processor id:%u has started-----\n",nCount);
				 
				 uint32_t nProcessed =
					ProcessFile(fileMode,strDbName,loadFromFileQ,nCount);
				 
				 dprintf(STDOUT_FILENO,"Processor id:%u has completed. Rows inserted:%u----\n",nCount, nProcessed);
			 });
	}
	
	dispatch_group_wait(fileProcGrp, DISPATCH_TIME_FOREVER);
	
    return PROGRAM_SUCCESS;
}


/* File Chunk processor - called by the dispatch group block to:
  - repeatedly load a chunk of (upto) 1MB from file
  - add the rows to db
 */
uint32_t ProcessFile(FILETYPE fileMode,const char* strDbName,dispatch_queue_t loadFromFileQ, uint16_t procId)
{
	uint32_t totalProcessed = 0;
	if( YES==AbortProgram ){ return totalProcessed; }
	
	PostgresConnection pgConnx;
	if( NO==pgConnx.Connect( strDbName) )
	{
		AbortProgram = YES;
		return totalProcessed;
	}
	PQsetClientEncoding(pgConnx, "UTF8" );
	
	// Allocate buffer for this Processor
	char* pWriteBuffer = (char*)malloc(OneMB+1);
	if( NULL==pWriteBuffer ) {
		AbortProgram = YES;
		return totalProcessed;
	}
	*(pWriteBuffer + OneMB) = '?';
	
	__block off_t nBytesRead;
	__block const char* endPos;
	__block off_t filePos; //start filepos
	BOOL didFail;
	
	while( NO==AbortProgram && FileBytesRemaining>0 )
	{
		dispatch_sync(loadFromFileQ,
					  ^{
						  nBytesRead = LoadFileBlock(pWriteBuffer, pWriteBuffer + OneMB, &endPos,&filePos);
					  });

		if( nBytesRead <=0 ) { return totalProcessed; }
		
		/*dprintf(STDOUT_FILENO,"(%u) Starting to process file offset:: %lld \n",
				procId, filePos);*/
		
		switch(fileMode)
		{
			case IPBLOCKS:{
				totalProcessed += ProcessBlocks(pWriteBuffer, endPos, pgConnx, &didFail);
				break;
			}
			case LOCATIONS:{
				totalProcessed += ProcessLocations(pWriteBuffer, endPos,pgConnx, &didFail);
				break;
			}
			default:{
				AbortProgram = YES;
				return totalProcessed;
			}
		}
		
		if( 0==(totalProcessed%2) ){
			dprintf(STDOUT_FILENO,"Processor:%u Total Processed:%u bytes remaining to process: %lld (%u MB)\n",
				procId, totalProcessed, (long long)FileBytesRemaining, (uint32_t)(FileBytesRemaining/OneMB) );
		}
		if( YES==didFail )
		{
			AbortProgram = YES;
			return totalProcessed;
		}
	};
	
	return totalProcessed;
}

static const char* BlocksHeader
= "network,geoname_id,registered_country_geoname_id,represented_country_geoname_id,is_anonymous_proxy,is_satellite_provider,postal_code,latitude,longitude,accuracy_radius";

static const char LocationsHeader[]
= "geoname_id,locale_code,continent_code,continent_name,country_iso_code,country_name,subdivision_1_iso_code,subdivision_1_name,subdivision_2_iso_code,subdivision_2_name,city_name,metro_code,time_zone";

/*
 - fdInputFile : Input file file descriptor.
 
 - Returns : [IPBLOCKS|LOCATIONS]
 */
BOOL ReadHeader( int fdInputFile, FILETYPE* pFileMode, uint16_t* pHeaderSize)
{
	// read and char scan the first line, up to n bytes looking for
	// newline '\n ' char.
	uint16_t nMaxChars = sizeof(LocationsHeader);
	char buffer[ nMaxChars ];
	char* pWritepos = buffer;
	const char* strHeader = &buffer[0];
	
	uint16_t nCharsRead = 0;
	off_t nBytesRead;
	while( nCharsRead<nMaxChars )
	{
		nBytesRead = read(fdInputFile,pWritepos,1);
		if(-1==nBytesRead ){
			perror("Error on reading .csv file.");
			return NO;
		}
		
		nCharsRead += nBytesRead;
		if( '\n' == *pWritepos ){
			break;
		}
		++pWritepos;
	}
	*pWritepos = '\0';
	*pHeaderSize = nCharsRead;
	
	
	if(0==strcmp(strHeader,BlocksHeader) ){
		*pFileMode = IPBLOCKS;
	}
	else if( 0==strcmp(strHeader,LocationsHeader) ){
		*pFileMode = LOCATIONS;
	}
	else{
		dprintf( STDOUT_FILENO,
				"geoimport - invalid .csv file, header not from a recognised source (Locations or IP Blocks).\n" );
		return NO;
	}
	
	return YES; //;  //=0,
}


/*
 Loads up to 1MB Chunk of file
 - pWriteBuffer : Pointer to the char buffer we can fill.
 - endPos : Pointer to the last character in the buffer (i.e. pWriteBuffer + OneMB)
 - pOutEndPos : out pointer that will point to the adjusted end position.
 
 - Returns : the bytes read from disk, or -1 if an error ocurred.
 */
off_t LoadFileBlock( char* pWriteBuffer, const char* endPos, const char** ppOutEndPos, off_t* filePos)
{
	*filePos = lseek(InputFile, 0, SEEK_CUR); //for debug
	
	ptrdiff_t sizeBuff = (endPos-pWriteBuffer);
	
	off_t nBytesRead = read(InputFile,pWriteBuffer,sizeBuff);
	if(-1==nBytesRead ){
		perror("Error on reading .csv file.");
		return nBytesRead;
	}
	if( 0==nBytesRead ){ return nBytesRead; }

	endPos = (pWriteBuffer + nBytesRead);
	
	// Adjust the file pointer and buffer end ptr to backtrack to the
	// end of a line - so buffer's read from file always start at a line beginning
	*ppOutEndPos = AdjustEndPointer(InputFile,pWriteBuffer,endPos);
	
	sizeBuff = (*ppOutEndPos - pWriteBuffer);
	
	FileBytesRemaining -= sizeBuff;
	return sizeBuff;
}


const char* ScanWordUnquoted(char* currentPos, char** ppCurrentPos);
const char* ScanWordMaybeQuoted(char* currentPos, char** ppCurrentPos);
const char* ScanWordUntil(char* currentPos, char** ppCurrentPos, char until);

static const char* country_unknown = "Unknown";
static const char* country_code_unknown = "ZZ";
static const char* strNoneProcessed = "Nobe Processed";

/*
	Reads all lines and proceses them as Location
*/
uint32_t ProcessLocations(char* currentPos, const char* endPos, PGconn* PqConn, BOOL* didFail)
{
	*didFail = NO;
	const char *geoname_id,*locale_code,*continent_code,*continent_name;
	const char *country_iso_code,*country_name,*subdivision_1_iso_code,*subdivision_1_name;
	const char *subdivision_2_iso_code,*subdivision_2_name,*city_name,*metro_code,*time_zone;
	
	const char* start_geoname_id = NULL;
	
	BOOL dbCallFailed;
	geoname_id = strNoneProcessed;
	uint32_t totalProcessed = 0;
	
	while(currentPos<endPos-1 )
	{
		char* lineStart = ReadLine(&currentPos,endPos);
		char* linePos = lineStart; //track each field read from the line
		
		// extract the details
		/*
		 geoname_id,locale_code,continent_code,continent_name,country_iso_code,country_name    ,subdivision_1_iso_code,subdivision_1_name  ,subdivision_2_iso_code ,subdivision_2_name  ,city_name       ,metro_code ,time_zone
		 88319,     en,         AF,            Africa,        LY,              Libya           ,BA                    ,"Sha'biyat Banghazi",                       ,                    ,Benghazi        ,           ,Africa/Tripoli
		 90552     ,en         ,AS            ,Asia          ,IQ              ,Iraq            ,AN                    ,Anbar               ,                       ,                    ,Sulaymaniyah    ,           ,Asia/Baghdad
		 105072    ,en         ,AS            ,Asia          ,SA              ,"Saudi Arabia"  ,14                    ,'Asir               ,                       ,                    ,"Khamis Mushait",           ,Asia/Riyadh
		 3333231   ,en		   ,EU			  ,Europe        ,GB              ,"United Kingdom",SCT                   ,Scotland            ,GLG                    ,"Glasgow City"      ,                ,           ,Europe/London
		 312394    ,en         ,AS            ,Asia          ,TR              ,Turkey          ,31                    ,Hatay               ,                       ,                    ,                ,           ,Europe/Istanbul
		*/
		
		geoname_id = ScanWordUnquoted(linePos,&linePos);
		locale_code = ScanWordUnquoted(linePos,&linePos);
		continent_code = ScanWordUnquoted(linePos,&linePos);
		continent_name = ScanWordMaybeQuoted(linePos,&linePos);
		country_iso_code = ScanWordUnquoted(linePos,&linePos);
		country_name = ScanWordMaybeQuoted(linePos,&linePos);
		subdivision_1_iso_code = ScanWordUnquoted(linePos,&linePos);
		subdivision_1_name = ScanWordMaybeQuoted(linePos,&linePos);
		subdivision_2_iso_code = ScanWordUnquoted(linePos,&linePos);
		subdivision_2_name = ScanWordMaybeQuoted(linePos,&linePos);
		city_name = ScanWordMaybeQuoted(linePos,&linePos);
		metro_code = ScanWordUnquoted(linePos,&linePos);
		time_zone = ScanWordUntil(linePos,&linePos, '\0');
		
		if( NULL==start_geoname_id) { start_geoname_id=geoname_id; }
		
		// apease the compiler warnings.
		locale_code = continent_name = metro_code = time_zone;
		
		if( NULL==country_iso_code ) { country_iso_code=country_code_unknown; }
		if( NULL==country_name ) { country_name = country_unknown; }
		
		dbCallFailed =
			AddLocationToDatabase( geoname_id, continent_code, city_name,
								   country_iso_code, country_name,
								   subdivision_1_iso_code, subdivision_1_name,
								   subdivision_2_iso_code, subdivision_2_name,
								   PqConn);
		
		if(YES==dbCallFailed){
			*didFail = YES;
			
			dprintf( STDOUT_FILENO, "Block Complete on Error. nLocationsProcessed = %u; firs id:%s , last id:%s\n",
					totalProcessed,start_geoname_id, geoname_id);
			
			return totalProcessed;
		}
		
		++totalProcessed;
	}
	
	/*dprintf( STDOUT_FILENO, "Block Complete, nLocationsProcessed = %u; firs id:%s , last id:%s\n",
			nLocationsProcessed,start_geoname_id, geoname_id);*/
	
	return totalProcessed;
}

uint32_t ProcessBlocks(char* currentPos, const char* endPos, PGconn* PqConn, BOOL* didFail)
{
	*didFail = NO;
	uint32_t totalProcessed = 0;
	const char *network, *geoname_id,*registered_country_geoname_id;
	const char *represented_country_geoname_id,*is_anonymous_proxy;
	const char *is_satellite_provider,*postal_code;
	const char *latitude,*longitude,*accuracy_radius;
	
	/*
		network          ,geoname_id ,registered_country_geoname_id ,represented_country_geoname_id ,is_anonymous_proxy ,is_satellite_provider ,postal_code ,latitude ,longitude ,accuracy_radius
		5.145.149.142/32 ,           ,2921044                       ,                               ,0                  ,1                     ,            ,         ,          ,
		80.231.5.0/24    ,           ,                              ,                               ,0                  ,1                     ,            ,         ,          ,
	*/
	
	geoname_id = strNoneProcessed;
	BOOL dbCallFailed = NO;
	while(currentPos<endPos-1 )
	{
		char* lineStart = ReadLine(&currentPos,endPos);
		char* linePos = lineStart; //track each field read from the line
		
		network =  ScanWordUnquoted(linePos,&linePos);
		geoname_id =  ScanWordUnquoted(linePos,&linePos);
		registered_country_geoname_id =  ScanWordUnquoted(linePos,&linePos);
		represented_country_geoname_id = ScanWordUnquoted(linePos,&linePos);
		is_anonymous_proxy = ScanWordUnquoted(linePos,&linePos);
		is_satellite_provider = ScanWordUnquoted(linePos,&linePos);
		postal_code = ScanWordUnquoted(linePos,&linePos);
		latitude = ScanWordUnquoted(linePos,&linePos);
		longitude = ScanWordUnquoted(linePos,&linePos);
		accuracy_radius = ScanWordUntil(linePos,&linePos, '\0');
		
		if( NULL==geoname_id ){
			geoname_id = registered_country_geoname_id;
			if( NULL==geoname_id ){
				geoname_id = represented_country_geoname_id;
			}
			if( NULL==geoname_id ){
				// Skip entries with no geoname id
				continue;
			}
		}
		
		dbCallFailed = AddIPBlockToDatabase(network, geoname_id, postal_code, PqConn);
		if(YES==dbCallFailed){
			*didFail = YES;
			return totalProcessed;
		}

		++totalProcessed;
	}
	/*dprintf( STDOUT_FILENO, "GEOIP Block Complete, Number of rows: %u; last ip block:%s\n",
			 totalProcessed,network );*/
	
	return totalProcessed;
}

const char* ScanWordUntil(char* currentPos, char** ppCurrentPos, char until)
{
	if( ','==*currentPos )
	{
		// this was an empty string
		*ppCurrentPos = (currentPos+1);
		return NULL;
	}
	
	const char* startPos = currentPos;
	while( until !=*currentPos )
	{
		++currentPos;
	}
	
	*currentPos = '\0';
	*ppCurrentPos = (currentPos+1);
	return startPos;
}

const char* ScanWordMaybeQuoted(char* currentPos, char** ppCurrentPos)
{
	if( ','==*currentPos )
	{
		// this was an empty string
		*ppCurrentPos = (currentPos+1);
		return NULL;
	}
	uint16_t nQuotes = 0;
	char cTerminator = ',';
	
	const char* startPos = currentPos;
	while( cTerminator !=*currentPos )
	{
		if( '"'==*currentPos )
		{
			if( startPos==currentPos){ //skip the first quote
				startPos+=1;
				cTerminator = '"';
			}
			++nQuotes;
		}
		
		++currentPos;
	}
	
	// todo - error if quotes not balanced?
	if( '"'==cTerminator ){
		*currentPos = '\0'; // terminated at the closing "
		currentPos++;		//skip the ,
	}
	
	*currentPos = '\0';
	*ppCurrentPos = (currentPos+1);
	return startPos;
}

/* Does NOT look for quotes.
 
	Scans the string up until the comma char, which is then replaced
	with a '\0' and the currentPos returned as the following char.
 
 ppCurrentPos = [out] the next character to be scanned
 
 Returns the start of the string sequence.
 */
const char* ScanWordUnquoted(char* currentPos, char** ppCurrentPos)
{
	if( ','==*currentPos )
	{
		// this was an empty string
		*ppCurrentPos = (currentPos+1);
		return NULL;
	}
	
	const char* startPos = currentPos;
	while( ',' !=*currentPos )
	{
		++currentPos;
	}
	
	*currentPos = '\0';
	*ppCurrentPos = (currentPos+1);
	return startPos;
}

void ProgramCleanup(void)
{
	if( InputFile>0 ){ close(InputFile); }
	
	dprintf( STDOUT_FILENO,"geoimport - program end.\n" );
}

const char* AdjustEndPointer(int fdInputFile, const char* pBufferStart, const char* endPos)
{
	if( '\n'==*endPos ){ return endPos; } //no adjustment needed.
	
	char* endPtr = (char*)endPos;
	
	while( endPtr>pBufferStart )
	{
		if( '\n' == *endPtr ){ break; }
		
		*endPtr = 'X';
		--endPtr;
	}
	++endPtr; //skip forwards to pass the \n
	off_t offset = (endPtr - endPos);
	
	// track current filepointer back so next read starts at the beginning of a line.
	//off_t currOffset =
	lseek(fdInputFile, offset, SEEK_CUR);
	
	return endPtr;
}


/*
	Reads the input until max chars reached, or newline.
 
	Sets the newline char to \0  and passes back pointer to start of null
	terminated line.
 
	adjusts ppCurPos with the new position
	decrements chars available.
 */
char* ReadLine( char** ppCurPos, const char* endPos )
{
	char* curPos = *ppCurPos;
	char* lineStart = curPos;
	
	// scan forward looking for the \r\n (or just \n)
	while( curPos<endPos )
	{
		if( '\n'==*curPos ){
			// null terminate the found line so we can str functions on it.
			*curPos = '\0';
			++curPos;
			break;
		}
		++curPos;
	}
	
	*ppCurPos = curPos;
	return lineStart;
}

static char PGConnectionString[512];

/*
 looks for the dbname and filename of .csv
 
 geoimport -P4 -D [dbname] /file/to/import.csv
 -
 geoimport -P4 -U 'postgresql://user@localhost/mydb?connect_timeout=10&application_name=myapp' /file/to/import.csv
 
 returns: 0 if OK
 otherwise something else
 */
int GetCommandlineOptions(int argc,
						  const char* argv[],
						  const char** strDbName,
						  const char** strFilename,
						  const char** strConnxString )
{
	int nIdx = 0;
	*strDbName = NULL;
	*strConnxString = NULL;
	
	while( --argc>1 )
	{
		const char* strCmd = argv[nIdx++];
		if( '-'!= *strCmd ){ return Usage(); }
		++strCmd;
		
		switch( *strCmd )
		{
			case 'D':
			{
				if( NULL!=*strConnxString){
					dprintf(STDOUT_FILENO, "Cannot combine -D with -U options.\n");
					return Usage();
				}
				
				// take the database name
				*strDbName = argv[nIdx++]; //assign
				snprintf( PGConnectionString, sizeof(PGConnectionString), "host=localhost dbname=%s", *strDbName );
				continue;
			}
			case 'P':{
				++strCmd;
				
				char* endptr;
				long lNumProcs = strtol(strCmd,&endptr,10);
				if( lNumProcs>0 && lNumProcs<=999 ){
					NumProcessors = lNumProcs;
				}
				continue;
			}
			case 'U':{
				if( NULL!=*strDbName){
					dprintf(STDOUT_FILENO, "Cannot combine -U with -D options.\n");
					return Usage();
				}
				strCmd = argv[nIdx++];
				
				size_t nLen = strlen(strCmd);
				if( nLen<=1 || nLen>=sizeof(PGConnectionString) ){
					dprintf(STDOUT_FILENO, "Invalid -U [connection string] .\n");
					return Usage();
				}
				nLen = strlcpy(&PGConnectionString[0], strCmd, nLen+1 );
				
				*strConnxString = &PGConnectionString[0];
				continue;
			}
			default:
			{
				dprintf( STDOUT_FILENO, "Error: Unrecognised option: %s\n\n", strCmd );
				return Usage();
			}
		}
	}
	
	*strFilename = argv[nIdx++]; //assign
	
	return 0;
}

int Usage()
{
	dprintf( STDOUT_FILENO,
		"Max Mind GEO IP Data importer.\nPopulates Geo ip tables on Postgres from a .csv file.\n\n" );
	
	dprintf( STDOUT_FILENO,
		"Options:\n\t-D Specify database name. Host will be localhost and user will be login of user executing the program.\n" );
	dprintf( STDOUT_FILENO,
			"\tUse this to connect to your local database server as yourself.\n" );
	
	dprintf( STDOUT_FILENO,
			"\n\t-P[1-999] Specify number of processing threads. Default will be up to 3 if not specified.\n" );
	
	dprintf( STDOUT_FILENO,
			"\n\t-U Specify Postgres connection string OR URL (cannot be used in conjunction with -D).\n" );
	dprintf( STDOUT_FILENO,
			"\tConnection string: 'host=localhost port=5432 dbname=mydb connect_timeout=10' \n" );
	dprintf( STDOUT_FILENO,
			"\t  * For details see: https://www.postgresql.org/docs/current/static/libpq-connect.html#LIBPQ-PARAMKEYWORDS \n" );
	
	dprintf( STDOUT_FILENO,
			"\tOR\n\tConnection URI: 'postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&...]' \n\n" );
	
	dprintf( STDOUT_FILENO,
			"Usage:\tgeoimport -P4 -D [dbname] /file/to/import.csv\n" );
	dprintf( STDOUT_FILENO,
			"Usage:\tgeoimport -P4 -U 'host=localhost port=5432 dbname=mydb connect_timeout=10' /file/to/import.csv\n" );
	dprintf( STDOUT_FILENO,
			"Usage:\tgeoimport -P4 -U 'postgresql://user@localhost/mydb?connect_timeout=10&application_name=myapp' /file/to/import.csv\n\n" );
	
	return 1;
}


BOOL PostgresConnection::Connect(const char* strDbName)
{
	
	char* errMsg = NULL;
	PQconninfoOption* cxnInfo = PQconninfoParse( PGConnectionString, &errMsg );
	PQconninfoFree(cxnInfo);
	if( NULL!=errMsg )
	{
		dprintf( STDOUT_FILENO,
				 "Error parsing Connection String: %s\n", errMsg );
		
		PQfreemem(errMsg);
		return NO;
	}
	
	PGconn* PqConn = PQconnectdb( PGConnectionString );
	switch( PQstatus(PqConn) )
	{
		case CONNECTION_OK:
		case CONNECTION_MADE:
		case CONNECTION_AUTH_OK:{
			m_connx = PqConn;
			return YES;
		}
		default:
		{
			char errUnk[] = "Unknown error";
			errMsg = PQerrorMessage( PqConn );
			if( NULL==errMsg){
				errMsg = errUnk;
			}
			dprintf( STDOUT_FILENO,
				 "Connection failed: %s\n", errMsg );

			PQfinish(PqConn);
			PqConn = NULL;
			return NO;
		}
	}
}


/*			Postgres Database		*/
const char* ADDLOCSql = "SELECT add_geoname_location($1::INT4,$2,$3,$4,$5,$6,$7,$8,$9)";
const char* ADDGEOIPSql = "SELECT add_geoip($1::inet,$2,$3)";

/*const int PGPARAM_FORMAT_BIN = 1;
const int PGPARAM_FORMAT_STR = 0;*/

typedef enum ADDLOC_PARAMS {
	p_geoname_id = 0 ,
	p_continent_code ,
	p_city_name ,
	p_iso2_country_code,
	p_country_name,
	p_subdivision_1_iso_code,
	p_subdivision_1_name,
	p_subdivision_2_iso_code,
	p_subdivision_2_name,
	ADDLOC_NUM_PARAMS } ADDLOC_PARAM_ID;

static int ADDLOCparamLengths[ADDLOC_NUM_PARAMS] = {0};
static int ADDLOCparamFormats[ADDLOC_NUM_PARAMS] = {0};

typedef enum ADDGEOIP_PARAMS {
	ADDGEOIP_p_network = 0,
	ADDGEOIP_p_geoname_id,
	ADDGEOIP_p_postal_code,
	ADDGEOIP_NUM_PARAMS }ADDGEOIP_PARAM_ID;

static int ADDGEOIPparamLengths[ADDLOC_NUM_PARAMS] = {0};
static int ADDGEOIPparamFormats[ADDLOC_NUM_PARAMS] = {0};

BOOL AddIPBlockToDatabase(const char* network,
						  const char* geoname_id,
						  const char* postal_code,
						  PGconn* PqConn)
{
	const char* ADDGEOIPvalues[ADDLOC_NUM_PARAMS];
	
	ADDGEOIPvalues[ADDGEOIP_p_network] = network;
	ADDGEOIPvalues[ADDGEOIP_p_geoname_id] = geoname_id;
	ADDGEOIPvalues[ADDGEOIP_p_postal_code] = postal_code;
	
	BOOL bDidFail;
	PGresult* pgRes = PQexecParams(PqConn,
								   ADDGEOIPSql,	//command,
								   ADDGEOIP_NUM_PARAMS,
								   NULL,
								   ADDGEOIPvalues,
								   ADDGEOIPparamLengths,
								   ADDGEOIPparamFormats, 0);
	
	ExecStatusType resStatus = PQresultStatus( pgRes );
	switch( resStatus )
	{
		case PGRES_EMPTY_QUERY:
		case PGRES_COMMAND_OK:
		case PGRES_TUPLES_OK:{
			bDidFail = NO;
			break;
		}
		case PGRES_BAD_RESPONSE:
		case PGRES_FATAL_ERROR:{
			bDidFail = YES;
			break;
		}
		default:{
			bDidFail = YES;
			break;
		}
	}
	
	if( YES==bDidFail ){
		const char* strErrMsg = PQresultErrorMessage(pgRes);
		
		dprintf(STDOUT_FILENO, "Failed to add Geo IP value (%s) for geoname_id:%s - %s\n",
				network, geoname_id,strErrMsg);
	}
	PQclear(pgRes);
	
	return bDidFail;
}


BOOL AddLocationToDatabase(const char* geoname_id, const char* continent_code, const char* city_name,
						   const char* country_iso_code, const char* country_name,
						   const char* subdivision_1_iso_code, const char* subdivision_1_name,
						   const char* subdivision_2_iso_code, const char* subdivision_2_name,
						   PGconn* PqConn)
{
	const char* ADDLOCvalues[ADDLOC_NUM_PARAMS];
	ADDLOCvalues[p_geoname_id] = geoname_id;
	ADDLOCvalues[p_continent_code] = continent_code;
	ADDLOCvalues[p_city_name] = city_name;
	ADDLOCvalues[p_iso2_country_code] = country_iso_code;
	ADDLOCvalues[p_country_name] = country_name;
	ADDLOCvalues[p_subdivision_1_iso_code] = subdivision_1_iso_code;
	ADDLOCvalues[p_subdivision_1_name] = subdivision_1_name;
	ADDLOCvalues[p_subdivision_2_iso_code] = subdivision_2_iso_code;
	ADDLOCvalues[p_subdivision_2_name] = subdivision_2_name;
	
	BOOL bDidFail;
	PGresult* pgRes = PQexecParams(PqConn,
								   ADDLOCSql,	//command,
								   ADDLOC_NUM_PARAMS,
								   NULL,
								   ADDLOCvalues,
								   ADDLOCparamLengths,
								   ADDLOCparamFormats, 0);
	
	ExecStatusType resStatus = PQresultStatus( pgRes );
	switch( resStatus )
	{
		case PGRES_EMPTY_QUERY:
		case PGRES_COMMAND_OK:
		case PGRES_TUPLES_OK:{
			bDidFail = NO;
			break;
		}
		case PGRES_BAD_RESPONSE:
		case PGRES_FATAL_ERROR:{
			bDidFail = YES;
			break;
		}
		default:{
			bDidFail = YES;
			break;
		}
	}
	
	if( YES==bDidFail ){
		const char* strErrMsg = PQresultErrorMessage(pgRes);
		
		dprintf(STDOUT_FILENO, "Failed to add Geoname Location value for geoname_id:%s - %s\n",
				geoname_id,strErrMsg);
		
		dprintf(STDOUT_FILENO,"Values: '%s'\n'%s'\n'%s'\n'%s'\n'%s'\n'%s'\n'%s'\n'%s'\n",
		continent_code, city_name,
		 country_iso_code,  country_name,
		 subdivision_1_iso_code,  subdivision_1_name,
				subdivision_2_iso_code,  subdivision_2_name );
		
	}
	PQclear(pgRes);
	
	return bDidFail;
}
