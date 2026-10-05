$ErrorActionPreference='Stop'
[Console]::InputEncoding=[System.Text.UTF8Encoding]::new($false,$true)
[Console]::OutputEncoding=[System.Text.UTF8Encoding]::new($false,$true)
$request=[Console]::In.ReadToEnd()|ConvertFrom-Json
[Console]::Error.WriteLine('CGP_AUDIT phase=compile entries=0')
Add-Type -TypeDefinition @'
using System;
using System.IO;
using System.Collections.Generic;
public static class CgpStorageAudit {
 public static long Entries;
 public static Dictionary<string,long> Charges;
 public static long Audit(string root,long limit,string[] tracked) {
  Entries=0;Charges=new Dictionary<string,long>(StringComparer.OrdinalIgnoreCase);
  foreach(string key in tracked)Charges.Add(Path.GetFullPath(key),0);
  return Visit(new DirectoryInfo(root),limit,0);
 }
 private static long Visit(FileSystemInfo item,long limit,int depth) {
  if(depth>512)throw new IOException("Audit depth limit");
  if((item.Attributes & FileAttributes.ReparsePoint)!=0)throw new IOException("Storage links forbidden");
  if(++Entries>limit)throw new IOException("Audit entry limit");
  if((Entries & 16383)==0)Console.Error.WriteLine("CGP_AUDIT phase=walk entries="+Entries);
  long total=0;
  var file=item as FileInfo;
  if(file!=null)total=file.Length;
  else foreach(var child in ((DirectoryInfo)item).EnumerateFileSystemInfos())total=checked(total+Visit(child,limit,depth+1));
  if(Charges.ContainsKey(item.FullName))Charges[item.FullName]=total;
  return total;
 }
}
'@
$tracked=[string[]]@($request.tracked)
[Console]::Error.WriteLine('CGP_AUDIT phase=walk entries=0')
$total=[CgpStorageAudit]::Audit([string]$request.root,[long]$request.maxScanEntries,$tracked)
[Console]::Error.WriteLine('CGP_AUDIT phase=serialize entries='+[CgpStorageAudit]::Entries)
$rows=[System.Collections.Generic.List[object]]::new()
foreach($key in $tracked){$rows.Add([object[]]@($key,[CgpStorageAudit]::Charges[[System.IO.Path]::GetFullPath($key)]))}
@{entries=[CgpStorageAudit]::Entries;bytes=$total;charges=$rows.ToArray()}|ConvertTo-Json -Depth 5 -Compress
