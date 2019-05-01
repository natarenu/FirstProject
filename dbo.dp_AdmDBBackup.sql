IF EXISTS (SELECT * FROM dbo.sysobjects WHERE ID = object_id(N'[dbo].[dp_AdmDBBackup]') AND OBJECTPROPERTY(ID, N'IsProcedure') = 1)
DROP PROC [dbo].[dp_AdmDBBackup]
GO

SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO

CREATE PROC dbo.dp_AdmDBBackup
	@VendorType char(1) = NULL,
	@DBName varchar(100),
	@BackupType char(1),
	@DBBackupPath varchar(255) = NULL,
	@Cycles tinyint = 1
-- WITH ENCRYPTION
AS

/* ****************************************************************
Author:		Nataraja S. Sidgal
Creation Date:	10/4/2005
Description:	This proc will create a full/diff/log backup of a database on the server
Run Proc:	EXEC dp_AdmDBBackup 'S', 'DBAudit', 'D', 'D:\DBBackup\', 2
		EXEC dp_AdmDBBackup @DBName = 'DBAudit', @BackupType = 'D'
Comments:	BackupType = D - Full Backup, I - Diff Backup, L - Log Backup
		VendorType = L - LightSpeed, S - SQL Backup
		EXEC msdb..sp_Delete_Database_Backuphistory 'dbname' - to delete the backup history from the msdb database
		to check the status of the database light speed backup, exec master..xp_slsreadprogress @database = 'dbaudit'
Input:		@VendorType = litespeed or sql native, @DBName = 'DBAudit', @BackupType = full/diff, @DBBackupPath = path for backup,
		@Cycles = number of backup cycles to keep
Output:		
Change Log:	3/17/2006 Nataraja S. Sidgal
		Added Logic to backup via light speed or native sql backup
		10/5/2007 Nataraja S. Sidgal
		Added Logic to delete backup files based on number of cycles of backup to keep
		3/23/2010 Nataraja S. Sidgal
		Added logic if backup path is NULL then to get the backup path, vendor and cycles from table 
		tblDBBackup that needs to be populated, else all the parameters should be passed
		7/7/2010 Nataraja S. Sidgal
		Added compression to the native SQL backup
		1/2/2013 Nataraja S. Sidgal
		Added litespeed encryption key for backup, @SQLStrDBBackup changed to 1000
**************************************************************** */

SET NOCOUNT ON

--select @DBName = 'DBAudit', @BackupType = 'D', @DBBackupPath = 'd:\backup\'

-- declare the parameters
DECLARE
	@DateStamp char(12),
	@SQLStrDBBackup varchar(1000)

-- check to see if the db already is in the table if not insert it
insert into tblDBBackup
(DBName, VendorType, DBBackupPath, Cycles)
select   s1.name
	,(select top 1 VendorType from tblDBBackup) 
	,(select top 1 DBBackupPath from tblDBBackup) 
	,(select top 1 Cycles from tblDBBackup) 
from master..sysdatabases s1 (nolock)
left join tblDBBackup t1 (nolock) on s1.name = t1.dbname
where t1.dbname is NULL and s1.name = @DBName
order by dbid

-- get the backup path, vendor and no of cycles to keep
if @DBBackupPath is null
begin
	select	@VendorType = VendorType,
		@DBBackupPath = DBBackupPath,
		@Cycles = Cycles
	from tblDBBackup (nolock)
	where DBName = @DBName
end

-- verify if backup path has a back slash if not add it
if right(@DBBackupPath,1) <> '\'
set @DBBackupPath = @DBBackupPath + '\'

-- set up a value for the datetime stamp
SELECT @DateStamp = convert(varchar(4), datepart(yy, getdate())) +
	case len(datepart(mm, getdate()))
	when 2
	then convert(varchar(2), datepart(mm, getdate()))
	else '0' + convert(varchar(2), datepart(mm, getdate()))
	end +
	case len(datepart(dd, getdate()))
	when 2
	then convert(varchar(2), datepart(dd, getdate()))
	else '0' + convert(varchar(2), datepart(dd, getdate()))
	end +
	case len(datepart(hh, getdate()))
	when 2
	then convert(varchar(2), datepart(hh, getdate()))
	else '0' + convert(varchar(2), datepart(hh, getdate()))
	end +
	case len(datepart(mi, getdate()))
	when 2
	then convert(varchar(2), datepart(mi, getdate()))
	else '0' + convert(varchar(2), datepart(mi, getdate()))
	end

-- set up sql scripts to backup the database.  database backups are overwritten to the disk
 -- full backup sql script
IF @BackupType = 'D'
BEGIN
  IF @VendorType = 'S'
    BEGIN
        SELECT @SQLStrDBBackup = 'USE [' + @DBName + ']' + char(10) + char(13) + 'BACKUP DATABASE [' + @DBName + '] TO DISK = ' + char(39) + @DBBackupPath + @DBName + '_' + @DateStamp + '_Ful.BAK' + char(39) + ' WITH INIT, NAME = ''' + @DBName + @DateStamp + 'Backup = FULL'', FORMAT, STATS = 5, COMPRESSION'
    END
  ELSE
    BEGIN
	SELECT @SQLStrDBBackup = 'USE MASTER' + char(10) + char(13) + 'exec master.dbo.xp_backup_database @database = ''[' + @DBName + ']'', @filename = ' + char(39) + @DBBackupPath + @DBName + '_' + @DateStamp + '_LS_Ful.BAK' + char(39) + ', @with = ''INIT'', @With = ''STATS = 5'', @BackupName = ''' + @DBName + @DateStamp + 'LSBackup = FULL''' + ', @jobp  = ''WXUcPBVD/SZqVLuSWF3L/9SWP/11iZCcbB3h/lSFhVnjyVyIlyoALCt3j8zTsTVhL5j18S9JbYk0oP0vcEh2vw=='''
    END
END

 -- diff backup sql script
IF @BackupType = 'I'
BEGIN
  IF @VendorType = 'S'
    BEGIN
	SELECT @SQLStrDBBackup = 'USE [' + @DBName + ']' + char(10) + char(13) + 'BACKUP DATABASE [' + @DBName + '] TO DISK = ' + char(39) + @DBBackupPath + @DBName + '_' + @DateStamp + '_Dif.BAK' + char(39) + ' WITH DIFFERENTIAL, INIT, NAME = ''' + @DBName + @DateStamp + 'Backup = DIFF'', FORMAT, STATS = 5, COMPRESSION'
    END
  ELSE
    BEGIN
	SELECT @SQLStrDBBackup = 'USE MASTER' + char(10) + char(13) + 'exec master.dbo.xp_backup_database @database = ''[' + @DBName + ']'', @filename = ' + char(39) + @DBBackupPath + @DBName + '_' + @DateStamp + '_LS_Dif.BAK' + char(39) + ', @with = ''DIFFERENTIAL'', @with = ''INIT'', @With = ''STATS = 5'', @BackupName = ''' + @DBName + @DateStamp + 'LSBackup = DIFF''' + ', @jobp  = ''WXUcPBVD/SZqVLuSWF3L/9SWP/11iZCcbB3h/lSFhVnjyVyIlyoALCt3j8zTsTVhL5j18S9JbYk0oP0vcEh2vw=='''
    END
END

 -- log backup sql script
IF @BackupType = 'L'
BEGIN
  IF @VendorType = 'S'
    BEGIN
	SELECT @SQLStrDBBackup  ='USE [' + @DBName + ']' + char(10) + char(13) + 'BACKUP LOG [' +  @DBName + '] TO DISK = ' + char(39) + @DBBackupPath + @DBName + '_' + @DateStamp + '_Log.BAK' + char(39) + ' WITH INIT, NAME = ''' + @DBName + @DateStamp + 'Backup = LOG'', FORMAT, STATS = 5, COMPRESSION'
    END
  ELSE
    BEGIN
	SELECT @SQLStrDBBackup = 'USE MASTER' + char(10) + char(13) + 'exec master.dbo.xp_backup_log @database = ''[' + @DBName + ']'', @filename = ' + char(39) + @DBBackupPath + @DBName + '_' + @DateStamp + '_LS_Log.BAK' + char(39) + ', @with = ''INIT'', @With = ''STATS = 5'', @BackupName = ''' + @DBName + @DateStamp + 'LSBackup = LOG''' + ', @jobp  = ''WXUcPBVD/SZqVLuSWF3L/9SWP/11iZCcbB3h/lSFhVnjyVyIlyoALCt3j8zTsTVhL5j18S9JbYk0oP0vcEh2vw=='''
    END
END

-- create the db backup
--PRINT @SQLStrDBBackup
EXEC (@SQLStrDBBackup)

-- if @@error = 0
-- begin

-- delete the last full, all diffs and all log backups once a new full backup is successful
IF @BackupType = 'D'
BEGIN
	EXEC dp_AdmDBCleanBackup @DBName, @DBBackupPath, @Cycles
END
 
-- end

RETURN

GO
SET QUOTED_IDENTIFIER OFF
GO
SET ANSI_NULLS ON
GO

