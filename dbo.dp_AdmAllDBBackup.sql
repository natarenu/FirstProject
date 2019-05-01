<<<<<<< HEAD
IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = object_id(N'[dbo].[dp_AdmAllDBBackup]') AND OBJECTPROPERTY(id, N'IsProcedure') = 1)
DROP PROC [dbo].[dp_AdmAllDBBackup]
GO

SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO

CREATE PROC dbo.dp_AdmAllDBBackup
	@VendorType char(1) = NULL,
	@BackupType char(1),
	@DBBackupPath varchar(255)= NULL,
	@Cycles tinyint = 1
-- WITH ENCRYPTION
AS

/* ****************************************************************
Author:		Nataraja S. Sidgal
Creation Date:	10/6/2005
Description:	This proc will create a full/diff/log backup of all database on the server
Run Proc:	EXEC dp_AdmAllDBBackup 'S', 'D', 'D:\DBBackup\', 2
		EXEC dp_AdmAllDBBackup @BackupType = 'D'
Comments:	BackupType = D - Full Backup, I - Diff Backup, L - Log Backup
		VendorType = L - LightSpeed, S - SQL Backup
		EXEC msdb..sp_Delete_Database_Backuphistory 'dbname' - to delete the backup history from the msdb database
		to check the status of the database light speed backup, exec master..xp_slsreadprogress @database = 'dbaudit'
Input:		@VendorType = litespeed or sql native, @BackupType = full/diff/log, @DBBackupPath = path for backup, 
		@Cycles = number of backup cycles to keep
Output:		
Change Log:	3/17/2006 Nataraja S. Sidgal
		Added Logic to backup via light speed or native sql backup
		10/5/2007 Nataraja S. Sidgal
		Added Logic to delete backup files based on number of cycles of backup to keep
		5/13/2009 Nataraja S. Sidgal
		Added logic for Full or Diff or Log based on recovery type
		3/23/2010 Nataraja S. Sidgal
		Added logic if backup path is NULL then to get the backup path, vendor and cycles from table 
		tblDBBackup that needs to be populated, else all the parameters should be passed
		7/7/2010 Nataraja S. Sidgal
		Added compression to the native SQL backup
		1/2/2013 Nataraja S. Sidgal
		Added litespeed encryption key for backup, @SQLStrDBBackup changed to 1000
**************************************************************** */

SET NOCOUNT ON

-- declare the parameters
DECLARE @DBName varchar(100),
	@DateStamp char(12),
	@SQLStrDBBackup varchar(1000)

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

-- create cursor

if @BackupType = 'D'
BEGIN
declare crsdbname insensitive cursor for
SELECT name as DBName
FROM master..sysdatabases (nolock)
WHERE DATABASEPROPERTYEX(name, 'status') = 'ONLINE'
AND name not in ('tempdb', 'pubs', 'Northwind')
--AND status NOT IN (32,64, 128, 256, 512, 32768)
order by name
END

if @BackupType = 'I'
BEGIN
declare crsdbname insensitive cursor for
SELECT name as DBName
FROM master..sysdatabases (nolock)
WHERE DATABASEPROPERTYEX(name, 'status') = 'ONLINE'
AND name not in ('master', 'tempdb', 'model', 'pubs', 'Northwind', 'msdb', 'DBAudit', 'LiteSpeedLocal')
--AND status NOT IN (32,64, 128, 256, 512, 32768)
order by name
END

if @BackupType = 'L'
BEGIN
declare crsdbname insensitive cursor for
SELECT name as DBName
FROM master..sysdatabases (nolock)
WHERE DATABASEPROPERTYEX(name, 'status') = 'ONLINE' AND DATABASEPROPERTYEX(name, 'Recovery') = 'FULL'
AND name not in ('master', 'tempdb', 'model', 'pubs', 'Northwind', 'msdb', 'DBAudit', 'LiteSpeedLocal')
--AND status NOT IN (32,64, 128, 256, 512, 32768)
order by name
END

open crsdbname

fetch next from crsdbname into @DBName
while @@fetch_status = 0
begin

print 'Backup Running On ' + @DBName

-- check to see if all the dbs are already in the table if not insert it
insert into tblDBBackup
(DBName, VendorType, DBBackupPath, Cycles)
select   s1.name
	,(select top 1 VendorType from tblDBBackup) 
	,(select top 1 DBBackupPath from tblDBBackup) 
	,(select top 1 Cycles from tblDBBackup) 
from master..sysdatabases s1 (nolock)
left join tblDBBackup t1 (nolock) on s1.name = t1.dbname
where t1.dbname is NULL and s1.name <> 'tempdb'
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

fetch next from crsdbname into @DBName
end

close crsdbname
deallocate crsdbname

-- end

RETURN
GO

SET QUOTED_IDENTIFIER OFF
GO
SET ANSI_NULLS ON
GO

=======
IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = object_id(N'[dbo].[dp_AdmAllDBBackup]') AND OBJECTPROPERTY(id, N'IsProcedure') = 1)
DROP PROC [dbo].[dp_AdmAllDBBackup]
GO

SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO

CREATE PROC dbo.dp_AdmAllDBBackup
	@VendorType char(1) = NULL,
	@BackupType char(1),
	@DBBackupPath varchar(255)= NULL,
	@Cycles tinyint = 1
-- WITH ENCRYPTION
AS

/* ****************************************************************
Author:		Nataraja S. Sidgal
Creation Date:	10/6/2005
Description:	This proc will create a full/diff/log backup of all database on the server
Run Proc:	EXEC dp_AdmAllDBBackup 'S', 'D', 'D:\DBBackup\', 2
		EXEC dp_AdmAllDBBackup @BackupType = 'D'
Comments:	BackupType = D - Full Backup, I - Diff Backup, L - Log Backup
		VendorType = L - LightSpeed, S - SQL Backup
		EXEC msdb..sp_Delete_Database_Backuphistory 'dbname' - to delete the backup history from the msdb database
		to check the status of the database light speed backup, exec master..xp_slsreadprogress @database = 'dbaudit'
Input:		@VendorType = litespeed or sql native, @BackupType = full/diff/log, @DBBackupPath = path for backup, 
		@Cycles = number of backup cycles to keep
Output:		
Change Log:	3/17/2006 Nataraja S. Sidgal
		Added Logic to backup via light speed or native sql backup
		10/5/2007 Nataraja S. Sidgal
		Added Logic to delete backup files based on number of cycles of backup to keep
		5/13/2009 Nataraja S. Sidgal
		Added logic for Full or Diff or Log based on recovery type
		3/23/2010 Nataraja S. Sidgal
		Added logic if backup path is NULL then to get the backup path, vendor and cycles from table 
		tblDBBackup that needs to be populated, else all the parameters should be passed
		7/7/2010 Nataraja S. Sidgal
		Added compression to the native SQL backup
		1/2/2013 Nataraja S. Sidgal
		Added litespeed encryption key for backup, @SQLStrDBBackup changed to 1000
**************************************************************** */

SET NOCOUNT ON

-- declare the parameters
DECLARE @DBName varchar(100),
	@DateStamp char(12),
	@SQLStrDBBackup varchar(1000)

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

-- create cursor

if @BackupType = 'D'
BEGIN
declare crsdbname insensitive cursor for
SELECT name as DBName
FROM master..sysdatabases (nolock)
WHERE DATABASEPROPERTYEX(name, 'status') = 'ONLINE'
AND name not in ('tempdb', 'pubs', 'Northwind')
--AND status NOT IN (32,64, 128, 256, 512, 32768)
order by name
END

if @BackupType = 'I'
BEGIN
declare crsdbname insensitive cursor for
SELECT name as DBName
FROM master..sysdatabases (nolock)
WHERE DATABASEPROPERTYEX(name, 'status') = 'ONLINE'
AND name not in ('master', 'tempdb', 'model', 'pubs', 'Northwind', 'msdb', 'DBAudit', 'LiteSpeedLocal')
--AND status NOT IN (32,64, 128, 256, 512, 32768)
order by name
END

if @BackupType = 'L'
BEGIN
declare crsdbname insensitive cursor for
SELECT name as DBName
FROM master..sysdatabases (nolock)
WHERE DATABASEPROPERTYEX(name, 'status') = 'ONLINE' AND DATABASEPROPERTYEX(name, 'Recovery') = 'FULL'
AND name not in ('master', 'tempdb', 'model', 'pubs', 'Northwind', 'msdb', 'DBAudit', 'LiteSpeedLocal')
--AND status NOT IN (32,64, 128, 256, 512, 32768)
order by name
END

open crsdbname

fetch next from crsdbname into @DBName
while @@fetch_status = 0
begin

print 'Backup Running On ' + @DBName

-- check to see if all the dbs are already in the table if not insert it
insert into tblDBBackup
(DBName, VendorType, DBBackupPath, Cycles)
select   s1.name
	,(select top 1 VendorType from tblDBBackup) 
	,(select top 1 DBBackupPath from tblDBBackup) 
	,(select top 1 Cycles from tblDBBackup) 
from master..sysdatabases s1 (nolock)
left join tblDBBackup t1 (nolock) on s1.name = t1.dbname
where t1.dbname is NULL and s1.name <> 'tempdb'
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

fetch next from crsdbname into @DBName
end

close crsdbname
deallocate crsdbname

-- end

RETURN
GO

SET QUOTED_IDENTIFIER OFF
GO
SET ANSI_NULLS ON
GO

>>>>>>> 2c24ed0e3d45f8e3611642cc0d58169d6bd958a9
