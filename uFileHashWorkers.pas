unit uFileHashWorkers;

{$mode objfpc}{$H+}

interface

uses
  Classes, SysUtils, HlpHashFactory, HlpIHash, HlpIHashResult, HlpCRC;

type
  TFileHashResult = class
  public
    FileName: string;
    FilePath: string;
    FileSize: Int64;
    HashValue: string;
  end;

  TFileHashWorkerPool = class;

  TFileHashWorkerThread = class(TThread)
  private
    FPool: TFileHashWorkerPool;
  protected
    procedure Execute; override;
  public
    constructor Create(APool: TFileHashWorkerPool);
  end;

  TFileHashWorkerPool = class
  private
    FJobs: TThreadList;
    FResults: TThreadList;
    FWorkers: array of TFileHashWorkerThread;
    FInputFinished: Boolean;
    FCancelRequested: Boolean;
    FOutstanding: Integer;
    FAlgorithmIndex: Integer;
    function GetNextJob(out AJob: TFileHashResult): Boolean;
    function CancellationRequested: Boolean;
    procedure PublishResult(AResult: TFileHashResult);
    procedure HashFile(AResult: TFileHashResult);
  public
    constructor Create(AWorkerCount, AAlgorithmIndex: Integer);
    destructor Destroy; override;
    procedure AddFile(const AFileName, AFilePath: string; AFileSize: Int64);
    procedure FinishInput;
    procedure CancelPending;
    function TryGetResult(out AResult: TFileHashResult): Boolean;
    function OutstandingCount: Integer;
    function MaximumOutstanding: Integer;
    procedure WaitForWorkers;
  end;

implementation

const
  HashBufferSize = 64 * 1024;

constructor TFileHashWorkerThread.Create(APool: TFileHashWorkerPool);
begin
  inherited Create(True);
  FreeOnTerminate := False;
  FPool := APool;
  Start;
end;

procedure TFileHashWorkerThread.Execute;
var
  Job: TFileHashResult;
begin
  while FPool.GetNextJob(Job) do
  begin
    FPool.HashFile(Job);
    FPool.PublishResult(Job);
  end;
end;

constructor TFileHashWorkerPool.Create(AWorkerCount, AAlgorithmIndex: Integer);
var
  I: Integer;
begin
  inherited Create;
  if AWorkerCount < 1 then
    AWorkerCount := 1;
  FAlgorithmIndex := AAlgorithmIndex;
  FJobs := TThreadList.Create;
  FResults := TThreadList.Create;
  SetLength(FWorkers, AWorkerCount);
  for I := 0 to High(FWorkers) do
    FWorkers[I] := TFileHashWorkerThread.Create(Self);
end;

destructor TFileHashWorkerPool.Destroy;
var
  I: Integer;
  Items: TList;
begin
  FinishInput;
  WaitForWorkers;
  for I := 0 to High(FWorkers) do
    FWorkers[I].Free;

  Items := FJobs.LockList;
  try
    while Items.Count > 0 do
    begin
      TObject(Items[0]).Free;
      Items.Delete(0);
    end;
  finally
    FJobs.UnlockList;
  end;
  FJobs.Free;

  Items := FResults.LockList;
  try
    while Items.Count > 0 do
    begin
      TObject(Items[0]).Free;
      Items.Delete(0);
    end;
  finally
    FResults.UnlockList;
  end;
  FResults.Free;
  inherited Destroy;
end;

procedure TFileHashWorkerPool.AddFile(const AFileName, AFilePath: string;
  AFileSize: Int64);
var
  Job: TFileHashResult;
  Items: TList;
begin
  Job := TFileHashResult.Create;
  Job.FileName := AFileName;
  Job.FilePath := AFilePath;
  Job.FileSize := AFileSize;
  Items := FJobs.LockList;
  try
    Items.Add(Job);
  finally
    FJobs.UnlockList;
  end;
  Inc(FOutstanding);
end;

procedure TFileHashWorkerPool.FinishInput;
begin
  FJobs.LockList;
  try
    FInputFinished := True;
  finally
    FJobs.UnlockList;
  end;
end;

procedure TFileHashWorkerPool.CancelPending;
var
  Items: TList;
begin
  Items := FJobs.LockList;
  try
    FCancelRequested := True;
    while Items.Count > 0 do
    begin
      TObject(Items[0]).Free;
      Items.Delete(0);
      Dec(FOutstanding);
    end;
    FInputFinished := True;
  finally
    FJobs.UnlockList;
  end;
end;

function TFileHashWorkerPool.CancellationRequested: Boolean;
begin
  FJobs.LockList;
  try
    Result := FCancelRequested;
  finally
    FJobs.UnlockList;
  end;
end;

function TFileHashWorkerPool.GetNextJob(out AJob: TFileHashResult): Boolean;
var
  Items: TList;
begin
  repeat
    AJob := nil;
    Items := FJobs.LockList;
    try
      if Items.Count > 0 then
      begin
        AJob := TFileHashResult(Items[0]);
        Items.Delete(0);
      end
      else if FInputFinished then
        Exit(False);
    finally
      FJobs.UnlockList;
    end;
    if AJob <> nil then
      Exit(True);
    Sleep(1);
  until False;
end;

procedure TFileHashWorkerPool.PublishResult(AResult: TFileHashResult);
var
  Items: TList;
begin
  Items := FResults.LockList;
  try
    Items.Add(AResult);
  finally
    FResults.UnlockList;
  end;
end;

function TFileHashWorkerPool.TryGetResult(out AResult: TFileHashResult): Boolean;
var
  Items: TList;
begin
  AResult := nil;
  Items := FResults.LockList;
  try
    if Items.Count > 0 then
    begin
      AResult := TFileHashResult(Items[0]);
      Items.Delete(0);
    end;
  finally
    FResults.UnlockList;
  end;
  Result := AResult <> nil;
  if Result then
    Dec(FOutstanding);
end;

function TFileHashWorkerPool.OutstandingCount: Integer;
begin
  Result := FOutstanding;
end;

function TFileHashWorkerPool.MaximumOutstanding: Integer;
begin
  Result := Length(FWorkers) * 2;
end;

procedure TFileHashWorkerPool.WaitForWorkers;
var
  I: Integer;
begin
  for I := 0 to High(FWorkers) do
    FWorkers[I].WaitFor;
end;

procedure TFileHashWorkerPool.HashFile(AResult: TFileHashResult);
var
  Stream: TFileStream;
  Hash: IHash;
  HashResult: IHashResult;
  CRCStandard: TCRCStandard;
  Buffer: array[0..HashBufferSize - 1] of Byte;
  BytesRead: Integer;
begin
  AResult.HashValue := '';
  try
    Stream := TFileStream.Create(AResult.FileName, fmOpenRead or fmShareDenyNone);
    try
      if Stream.Size = 0 then
      begin
        AResult.HashValue := 'ZERO BYTE FILE';
        Exit;
      end;

      case FAlgorithmIndex of
        0: Hash := THashFactory.TCrypto.CreateMD5();
        1: Hash := THashFactory.TCrypto.CreateSHA1();
        2: Hash := THashFactory.TCrypto.CreateSHA3_256();
        3: Hash := THashFactory.TCrypto.CreateSHA2_256();
        4: Hash := THashFactory.TCrypto.CreateSHA2_512();
        5:
          {$ifdef CPU64}
          Hash := THashFactory.THash64.CreateXXHash64();
          {$else}
          Hash := THashFactory.THash32.CreateXXHash32();
          {$endif}
        6: Hash := THashFactory.THash64.CreateXXHash3();
        7: Hash := THashFactory.TCrypto.CreateBlake2B_256();
        8: Hash := THashFactory.TCrypto.CreateBlake3_256(nil);
        9:
          begin
            CRCStandard := TCRCStandard.CRC32;
            Hash := THashFactory.TChecksum.TCRC.CreateCRC(CRCStandard);
          end;
      else
        raise Exception.CreateFmt('Unsupported hash algorithm index: %d',
          [FAlgorithmIndex]);
      end;

      Hash.Initialize();
      repeat
        if CancellationRequested then
        begin
          AResult.HashValue := 'CANCELLED';
          Exit;
        end;
        BytesRead := Stream.Read(Buffer, SizeOf(Buffer));
        if BytesRead <= 0 then
          Break;
        Hash.TransformUntyped(Buffer, BytesRead);
      until False;
      HashResult := Hash.TransformFinal();
      AResult.HashValue := HashResult.ToString();
    finally
      Stream.Free;
    end;
  except
    on E: Exception do
      AResult.HashValue := 'ERROR: ' + E.Message;
  end;
end;

end.
