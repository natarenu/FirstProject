IF EXISTS (SELECT * FROM dbo.sysobjects WHERE ID = object_id(N'[dbo].[dp_AdmDBCleanBackup]') AND OBJECTPROPERTY(ID, N'IsProcedure') = 1)
DROP PROC [dbo].[dp_AdmDBCleanBackup]
GO

SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO

CREATE PROC dbo.dp_AdmDBCleanBackup
	@DBName varchar(100),
	@DBBackupPath varchar(255),
	@Cycles tinyint = 1
-- WITH ENCRYPTION
AS

/* ****************************************************************
Author:		Nataraja S. Sidgal
Creation Date:	10/5/2007
Description:	This proc will delete the backup files from the backup location and keep
		only the required number of cycles of backup
Run Proc:	EXEC dp_AdmDBCleanBackup 'DBAudit', 'D:\DBBackup\', 2
Comments:	
Input:		@DBName = 'DBAudit', @DBBackupPath = path for backup, @Cycles = no of cycles to keep
Output:		
Change Log:	Nataraja S. Sidgal 10/27/2008
		get the directory of the backup files that are archived
		Nataraja Sidgal 7/7/2014 changed the datatype to delete files for big DB names
**************************************************************** */

SET NOCOUNT ON

-- declare the parameters
--declare @DBName varchar(50), @DBBackupPath varchar(255), @Cycles tinyint
declare @DirString varchar(2000), @DelString varchar(1000), @FileName varchar(200)
declare @recordcount int

-- to get the list of full backups
create table #MediaSetID (id int identity (1,1), media_set_id int)

-- to get the list of files on the backup directory
create table #BackupDir (ID int identity(1,1), FileNames varchar(200))

-- insert to get the list of full backups
insert #MediaSetID (media_set_id)
select 	a.media_set_id
from msdb.dbo.backupset a (nolock)
inner join msdb..backupmediafamily b (nolock) on b.media_set_id = a.media_set_id
where b.media_set_id in (select media_set_id from msdb..backupset (nolock) where type = 'D')
and a.Database_Name = @dbname
group by a.database_name, b.physical_device_name, a.backup_size, a.backup_start_date, a.backup_finish_date, a.media_set_id
order by 1 desc

-- insert all the cycles of full, diff and log backups
select 	a.media_set_id, a.Database_Name DBName, substring(b.physical_device_name, len(@DBBackupPath)+1, 100) BackupLocation
into #BackupFile
from msdb.dbo.backupset a (nolock)
inner join msdb..backupmediafamily b (nolock) on b.media_set_id = a.media_set_id
where b.media_set_id >= (select media_set_id from #MediaSetID where id = @Cycles)
and a.Database_Name = @dbname
order by a.media_set_id

-- get the directory of the backup files that are archived
select @DirString = 'insert #BackupDir exec master.dbo.xp_cmdshell ''dir ' + @DBBackupPath + ' /a:-a'''
--'insert #BackupDir exec master.dbo.xp_cmdshell ''dir ' + @DBBackupPath + ''''

--print @DirString
exec (@DirString)

-- delete all the header and directory info
delete from #BackupDir
where substring(FileNames, 3, 1) <> '/' 
or FileNames is null or
substring(FileNames, 25, 1) = '<'

-- delete all the files that do not belong to the current database
--select @DirString = 'delete from #BackupDir where filenames not like ''%' + @DBName + '%'''
select @DirString = 'delete from #BackupDir where ltrim(rtrim(substring(FileNames, 40, len(''' + @DBName + ''')))) <> '''+ @DBName + ''''
--print @DirString
exec (@DirString)

-- delete all the files that have the same portion of the DB Name
select @DirString = 'delete from #BackupDir where isnumeric(ltrim(rtrim(substring(FileNames, 40 + len(''' + @DBName + ''') + 1, 1)))) = 0'
--print @DirString
exec (@DirString)

-- delete all the matching records in the 2 temp tables
delete #BackupDir
from #BackupFile a
inner join #BackupDir b on ltrim(rtrim(substring(b.FileNames, 40, 70))) = a.BackupLocation

select @recordcount = count(1) from #MediaSetID

if @recordcount >= @Cycles
begin

	-- declare a cursor to delete one file at a time
	Declare FileCursor cursor for
	select FileNames from #BackupDir
	
	open FileCursor 
	
	fetch next from FileCursor into @FileName
	while @@fetch_status = 0
	begin
	
	set @DelString = 'exec master.dbo.xp_cmdshell ''del ' + @DBBackupPath + ltrim(rtrim(substring(@FileName, 40, 200))) + ''''
	
	--print @DelString
	exec (@DelString)
	
	fetch next from FileCursor into @FileName
	end
	
	close FileCursor
	deallocate FileCursor

end

drop table #BackupDir

drop table #BackupFile

drop table #MediaSetID

RETURN


GO
