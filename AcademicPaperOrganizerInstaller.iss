; Academic Paper Organizer - show old version -> new version before update

#define MyAppName "Academic Paper Organizer"
#define MyAppVersion "1.0.1"
#define MyAppPublisher "In Ho Park"
#define MyAppURL "https://github.com/inhopark68/academic_paper_organizer"
#define MyAppExeName "AcademicPaperOrganizer.exe"
#define MySourceDir "D:\coding\academic_paper_organizer_windows_exe\academic_paper_organizer_windows_exe\dist\AcademicPaperOrganizer"

[Setup]
AppId={{8E2D8A6B-7A57-4EFA-9A50-8F7A7A5C1101}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppPublisher={#MyAppPublisher}
AppPublisherURL={#MyAppURL}
AppSupportURL={#MyAppURL}
AppUpdatesURL={#MyAppURL}

DefaultDirName={autopf}\{#MyAppName}
DefaultGroupName={#MyAppName}
DisableProgramGroupPage=yes

OutputDir=installer_output
OutputBaseFilename=AcademicPaperOrganizerSetup_{#MyAppVersion}

VersionInfoVersion={#MyAppVersion}
VersionInfoTextVersion={#MyAppVersion}
VersionInfoProductTextVersion={#MyAppVersion}

Compression=lzma
SolidCompression=yes
WizardStyle=modern

PrivilegesRequired=admin
ArchitecturesAllowed=x64
ArchitecturesInstallIn64BitMode=x64

UninstallDisplayIcon={app}\{#MyAppExeName}

CloseApplications=yes
RestartApplications=no
CloseApplicationsFilter=*.exe,*.dll,*.pyd

[Languages]
Name: "korean"; MessagesFile: "compiler:Languages\Korean.isl"

[Tasks]
Name: "desktopicon"; Description: "바탕화면 바로가기 만들기"; GroupDescription: "추가 작업:"; Flags: unchecked

[Files]
Source: "{#MySourceDir}\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
Name: "{group}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"
Name: "{autodesktop}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"; Tasks: desktopicon

[Run]
Filename: "{app}\{#MyAppExeName}"; Description: "{#MyAppName} 실행"; Flags: nowait postinstall skipifsilent

[UninstallDelete]
Type: filesandordirs; Name: "{app}\_internal"

[Code]
var
  InstalledVersion: string;

function GetInstalledVersion(): string;
var
  S: string;
begin
  Result := '';

  { 보통 머신 전체 설치(HKLM)부터 확인 }
  if RegQueryStringValue(
       HKLM,
       'SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\{{8E2D8A6B-7A57-4EFA-9A50-8F7A7A5C1101}_is1',
       'DisplayVersion',
       S) then
  begin
    Result := S;
    exit;
  end;

  { 경우에 따라 HKCU에 있을 수도 있음 }
  if RegQueryStringValue(
       HKCU,
       'SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\{{8E2D8A6B-7A57-4EFA-9A50-8F7A7A5C1101}_is1',
       'DisplayVersion',
       S) then
  begin
    Result := S;
    exit;
  end;

  { fallback: DisplayVersion이 없으면 DisplayName이라도 확인 가능 }
end;

function IsUpgradeInstall(): Boolean;
begin
  Result := GetInstalledVersion() <> '';
end;

function KillAppProcess(): Boolean;
var
  ResultCode: Integer;
begin
  Exec(
    ExpandConstant('{sys}\taskkill.exe'),
    '/IM "{#MyAppExeName}" /T /F',
    '',
    SW_HIDE,
    ewWaitUntilTerminated,
    ResultCode
  );
  Result := True;
end;

procedure InitializeWizard;
begin
  InstalledVersion := GetInstalledVersion();

  if InstalledVersion <> '' then
  begin
    SuppressibleMsgBox(
      '기존 설치가 감지되었습니다.' + #13#10#13#10 +
      '현재 설치된 버전: ' + InstalledVersion + #13#10 +
      '설치할 버전: {#MyAppVersion}' + #13#10#13#10 +
      '기존 설치 위치에 새 버전을 덮어써서 업데이트합니다.' + #13#10 +
      '설치 경로는 변경하지 않는 것을 권장합니다.',
      mbInformation,
      MB_OK,
      IDOK
    );
  end;
end;

function PrepareToInstall(var NeedsRestart: Boolean): String;
begin
  if InstalledVersion <> '' then
  begin
    if MsgBox(
      '업데이트를 진행합니다.' + #13#10#13#10 +
      '현재 설치된 버전: ' + InstalledVersion + #13#10 +
      '새 버전: {#MyAppVersion}' + #13#10#13#10 +
      '{#MyAppName}가 실행 중이면 자동으로 종료한 후 설치합니다.' + #13#10 +
      '계속하시겠습니까?',
      mbConfirmation,
      MB_YESNO
    ) = IDYES then
    begin
      KillAppProcess();
      Result := '';
    end
    else
    begin
      Result := '업데이트가 취소되었습니다.';
    end;
  end
  else
  begin
    Result := '';
  end;
end;