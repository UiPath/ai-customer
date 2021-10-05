#Requires -RunAsAdministrator

<#

.SYNOPSIS
   Makes aifabric related changes to orchestrator web.config to enable aifabric installation and access.
.DESCRIPTION
   Add entries in orchestrator web.config(if not exists) for orchestrator internal IDP and aifabric access from robot and orchestrator.
   Removes cache to allow access to new controllers and resets iis to load new values.
.NOTES
    Name: ./orchestratorAutomation.ps1
    Author: AIFabric Team
    Pre-Requisites: script has to be executed throuh powershell in Administrator mode & before running script set execution policy to RemoteSigned by running "Set-ExecutionPolicy RemoteSigned"
.EXAMPLE
    If aifabric is available at ww.xx.yy.zz:port, command to run would be
    .\orchestratorAutomation.ps1 -aifUrl ww.xx.yy.zz:port

    If ai-app is accessed via domain instead of IP:PORT combo, then enable domainBasedAccess to true
    .\orchestratorAutomation.ps1 -aifUrl "aif-sahil-aks.westeurope.cloudapp.azure.com"

    If aifabric is available at ww.xx.yy.zz:port and Orchestrator Installation Path has to be specified,
    .\orchestratorAutomation.ps1 -aifUrl ww.xx.yy.zz:port -config "C:\Program Files (x86)\UiPath\Orchestrator"

    If aifabric is available at ww.xx.yy.zz:port and aifabric storage is available at aa.bb.cc.dd:port,
    .\orchestratorAutomation.ps1 -aifUrl ww.xx.yy.zz:port -storageUrl aa.bb.cc.dd:port

#>

Param (
   [Parameter(Mandatory = $true, ValueFromPipelineByPropertyName)]
   [string] $aifUrl,
   [Parameter(Mandatory = $false, ValueFromPipelineByPropertyName)]
   [string] $config,
   [Parameter(Mandatory = $false, ValueFromPipelineByPropertyName)]
   [string] $storageUrl
)

Import-Module 'WebAdministration'


if(!$config){
    $config = "C:\Program Files (x86)\UiPath\Orchestrator"
}

#if path does not end with \ add it
if( $config -notmatch '\\$' ){
    $config += '\'
}

$dll_config = $config + 'UiPath.Orchestrator.dll.config'

#Fetching Orchestrator version
if(Test-Path $dll_config){
    $orchestrator_version = [System.Diagnostics.FileVersionInfo]::GetVersionInfo($config + 'UiPath.Orchestrator.web.dll').FileVersion
    echo "Orchestrator version : $orchestrator_version"
}

if(Test-Path $dll_config){
    $config = $config + 'UiPath.Orchestrator.dll.config'
    $configFile = 'UiPath.Orchestrator.dll.config'
} else{
    $config = $config + 'web.config'
    $configFile = 'web.config'
}

#Check for the existence of config file
if(-not (Test-Path $config)){
	throw "$config File does not Exists. Please make sure that the Orchestrator installation folder is correct !"
	exit
}

echo "Path to Web config: "$config

Copy-Item $config -Destination ("$config.original."+(Get-Date -Format "MMddyyyy.HH.mm.ss"))

#AiFabric Settings template
$STATIC_NODES_STRING='
<Collection>
    <add key="AiFabric.Licensing" value="true" />
    <add key="AiFabric.MLSkillsCreate" value="false" />
    <add key="AiFabric.MLSkillsCreateOOB" value="false" />
    <add key="AiFabric.PackagesCreate" value="false" />
    <add key="AiFabric.Packages" value="false" />
    <add key="AiFabric.Logs" value="false" />
    <add key="AiFabric.ModuleEnabled" value="true" />
    <add key="AiFabric.FeatureEnabledByDefault" value="true" />
    <add key="AiFabric.MLPackagingInstructionsUrl" value="https://docs.uipath.com/ai-fabric/docs/building-ml-packages" />
    <add key="AiFabric.MLServiceUrl" value="https://{{aifUrl}}" />
    <add key="AiFabric.MLSkillUrl" value="https://{{aifUrl}}/ai-deployer" />
    <add key="AiFabric.MLPackageUrl" value="https://{{aifUrl}}/ai-pkgmanager" />
    <add key="AiFabric.MLLogUrl" value="https://{{aifUrl}}/ai-helper" />
    <add key="AiFabric.MLTrainUrl" value="https://{{aifUrl}}/ai-trainer" />
    <add key="AiFabric.ModelStorageUrl" value="https://{{storageUrl}}" />
    <add key="AiFabric.AccountId" value="host" />
</Collection>'

if($aifUrl.StartsWith("http://") -or $aifUrl.StartsWith("https://"))
{
    echo "Parameter aifUrl should not start with http or https. Please provide url without scheme"
    throw "Invalid aifUrl input provided: $aifUrl"
}

if($storageUrl.StartsWith("http://") -or $storageUrl.StartsWith("https://"))
{
    echo "Parameter storageUrl should not start with http or https. Please provide url without scheme"
    throw "Invalid storageUrl input provided: $storageUrl"
}

# set nodes value
$STATIC_NODES_STRING = $STATIC_NODES_STRING.Replace("{{aifUrl}}",$aifUrl);
$STATIC_NODES_STRING = $STATIC_NODES_STRING.Replace("{{storageUrl}}",$storageUrl);
$STATIC_NODES = [xml]$STATIC_NODES_STRING

# edit web config
function AifabricFixedConfig
{
    $nodes = Select-Xml -XPath '//add' -Xml $STATIC_NODES

    $file = gi $config
    $xml = [xml](gc $file)
    foreach($node in $nodes)
    {
        #remove existing nodes if they exist. They should not.
        $key = $node.Node.key
        $xml.SelectNodes("configuration/appSettings/add[@key='$key']") | %{$xml.configuration.appSettings.RemoveChild($_)}
        $xml.configuration.appSettings.AppendChild($xml.ImportNode($node.Node,1))
    }

    $xml.Save($file.FullName)
}

# Reset IIS to reload values
function Retry-IISRESET
{
    param (
    [Parameter(Mandatory=$false)][int]$retries = 5,
    [Parameter(Mandatory=$false)][int]$secondsDelay = 2
    )

    $retrycount = 0
    $completed = $false

    while (-not $completed) {
        try {
            iisreset | Tee-Object -Variable statusiisreset
			if("$statusiisreset".Contains("failed"))
			{
				throw
			}
            Write-Verbose ("reset succeded")
            $completed = $true
        } catch {
            if ($retrycount -ge $retries) {
                throw
            } else {
                Start-Sleep $secondsDelay
                $retrycount++
            }
        }
    }
}

# remove cache
function EmptyAspNetCache
{
    $Framework32bitFolder = "\Framework\"
    $Framework64bitFolder = "\Framework64\"
    $temporaryAspNetFolder = "Temporary ASP.NET Files\root"
    $ControllerCacheFileName = "MS-ApiControllerTypeCache.xml"
    $aspNetCacheFolder = [System.Runtime.InteropServices.RuntimeEnvironment]::GetRuntimeDirectory()+$temporaryAspNetFolder
    if ([Environment]::Is64BitOperatingSystem)
    {
        $aspNetCacheFolder = $aspNetCacheFolder.Replace($Framework32bitFolder, $Framework64bitFolder);
    }
    if (!(Test-Path $aspNetCacheFolder))
    {
        echo $"Folder $aspNetCacheFolder not found for removing $ControllerCacheFileName"
        return
    }
    echo $"Removing $ControllerCacheFileName files from ASP.NET cache folder $aspNetCacheFolder"
    Get-Childitem -Path $aspNetCacheFolder -Include $ControllerCacheFileName -Recurse | ForEach {
        $retrycount = 0
        $retries = 3
        $completed = $false

        while (-not $completed) {
            try {
                Remove-Item $_.FullName
                $completed = $true
            } catch {
                if ($retrycount -ge $retries) {
                    throw
                } else {
                    Start-Sleep 2
                    $retrycount++
                }
            }
        }

        echo $"Removed $ControllerCacheFileName"
    }
}

#create the proper web.config with configuration
AifabricFixedConfig
EmptyAspNetCache
Retry-IISRESET 3 2
Sleep 2
echo "Orchestrator configured successfully"