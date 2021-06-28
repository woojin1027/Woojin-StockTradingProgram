# -*-coding: utf-8 -*-
from PyQt5.QtCore import QObject, pyqtSlot
from PyQt5.QtWidgets import QMainWindow, QApplication
from PyQt5.QAxContainer import QAxWidget
from PyQt5.QtCore import QThread
from threading import Lock, Thread
from collections import deque
from sys import argv
from time import sleep
from json import dumps
from os import kill, getpid


class SyncRequestDecorator:
    """키움 API 비동기 함수 데코레이터
    """

    @staticmethod
    def kiwoom_sync_request(func):
        def func_wrapper(self, *args, **kwargs):
            self.request_thread_worker.request_queue.append((func, args, kwargs))

        return func_wrapper

    @staticmethod
    def kiwoom_sync_callback(func):
        def func_wrapper(self, *args, **kwargs):
            # print("[%s] 키움 함수 콜백: %s %s" % (func.__name__, args, kwargs))

            func(self, *args, **kwargs)  # 콜백 함수 호출
            if self.request_thread_worker.request_thread_lock.locked():
                self.request_thread_worker.request_thread_lock.release()  # 요청 쓰레드 잠금 해제

            # 재시도 횟수 초기화
            myWindow._kiwoom.request_thread_worker.retry_count = 0

        return func_wrapper


class RequestThreadWorker(QObject):
    def __init__(self):
        """요청 쓰레드
        """
        super().__init__()
        self.request_queue = deque()
        self.request_thread_lock = Lock()

        # 간혹 요청에 대한 결과가 콜백으로 오지 않음
        # 마지막 요청을 저장해 뒀다가 일정 시간이 지나도 결과가 안오면 재요청
        self.retry_timer = None
        self.max_retry = 3
        self.retry_count = 0

    def retry(self, request):
        self.retry_count = self.retry_count + 1

        if self.retry_count == self.max_retry:
            kill(getpid(), 9)

        # print("[%s] 키움 함수 재시도: %s %s" % (request[0].__name__, request[1], request[2]))

        self.request_queue.appendleft(request)

    def run(self):
        last_request = None
        while True:
            # 큐에 요청이 있으면 하나 뺌
            # 없으면 블락상태로 있음

            try:
                request = self.request_queue.popleft()
            except IndexError as e:
                if self.request_thread_lock.locked():
                    if not self.request_thread_lock.acquire(blocking=True, timeout=5):
                        self.request_thread_lock.release()
                        self.retry(last_request)
                    else:
                        self.request_thread_lock.release()

                sleep(0.2)
                continue

            # 요청에대한 결과 대기
            if not self.request_thread_lock.acquire(blocking=True, timeout=5):
                if self.request_thread_lock.locked():
                    self.request_thread_lock.release()
                # 요청 실패
                sleep(0.2)

                self.request_queue.appendleft(request)
                self.retry(last_request)  # 실패한 요청 재시도
            else:
                last_request = request
                # 요청 실행
                # print("[%s] 키움 함수 실행: %s %s" % (request[0].__name__, request[1], request[2]))
                request[0](trader, *request[1], **request[2])

            sleep(1)  # 0.2초 이상 대기 후 마무리


class Kiwoom(QObject):
    # Variables

    def __init__(self):
        super().__init__()
        self.ocx = QAxWidget("KHOPENAPI.KHOpenAPICtrl.1")
        self.ocx.OnEventConnect[int].connect(self.OnEventConnect)
        self.ocx.OnReceiveMsg[str, str, str, str].connect(self.OnReceiveMsg)
        self.ocx.OnReceiveTrData[str, str, str, str, str, int, str, str, str].connect(self.OnReceiveTrData)
        self.ocx.OnReceiveRealData[str, str, str].connect(self.OnReceiveRealData)
        self.ocx.OnReceiveChejanData[str, int, str].connect(self.OnReceiveChejanData)
        self.ocx.OnReceiveConditionVer[int, str].connect(self.OnReceiveConditionVer)
        self.ocx.OnReceiveTrCondition[str, str, str, int, int].connect(self.OnReceiveTrCondition)
        self.ocx.OnReceiveRealCondition[str, str, str, str].connect(self.OnReceiveRealCondition)

        # 요청 쓰레드
        self.request_thread_worker = RequestThreadWorker()
        self.request_thread = QThread()
        self.request_thread_worker.moveToThread(self.request_thread)
        self.request_thread.started.connect(self.request_thread_worker.run)
        self.request_thread.start()

    # 로그인
    # 0 - 성공, 음수값은 실패
    @pyqtSlot(result=int)
    def CommConnect(self):
        return self.ocx.dynamicCall("CommConnect()")

    # 로그인 상태 확인
    # 0:미연결, 1:연결완료, 그외는 에러
    @pyqtSlot(result=int)
    def GetConnectState(self):
        res = self.ocx.dynamicCall("GetConnectState()")
        if res == 1:
            print('Online')
        else:
            print('Offline')
        return res

    # 로그 아웃
    @pyqtSlot()
    def CommTerminate(self):
        self.ocx.dynamicCall("CommTerminate()")

    # 로그인한 사용자 정보를 반환한다.
    # “ACCOUNT_CNT” – 전체 계좌 개수를 반환한다.
    # "ACCNO" – 전체 계좌를 반환한다. 계좌별 구분은 ‘;’이다.
    # “USER_ID” - 사용자 ID를 반환한다.
    # “USER_NAME” – 사용자명을 반환한다.
    # “KEY_BSECGB” – 키보드보안 해지여부. 0:정상, 1:해지
    # “FIREW_SECGB” – 방화벽 설정 여부. 0:미설정, 1:설정, 2:해지
    @pyqtSlot(str, result=str)
    def GetLoginInfo(self, tag):
        return self.ocx.dynamicCall("GetLoginInfo(QString)", [tag])

    # Tran 입력 값을 서버통신 전에 입력값일 저장한다.
    @pyqtSlot(str, str)
    def SetInputValue(self, id, value):
        self.ocx.dynamicCall("SetInputValue(QString, QString)", [id, value])

    # 통신 데이터를 송신한다.
    # 0이면 정상
    # OP_ERR_SISE_OVERFLOW – 과도한 시세조회로 인한 통신불가
    # OP_ERR_RQ_STRUCT_FAIL – 입력 구조체 생성 실패
    # OP_ERR_RQ_STRING_FAIL – 요청전문 작성 실패
    # OP_ERR_NONE – 정상처리
    @pyqtSlot(str, str, int, str, result=int)
    def CommRqData(self, rQName, trCode, prevNext, screenNo):
        return self.ocx.dynamicCall("CommRqData(QString, QString, int, QString)", [rQName, trCode, prevNext, screenNo])

    # 수신 받은 데이터의 반복 개수를 반환한다.
    @pyqtSlot(str, str, result=int)
    def GetRepeatCnt(self, trCode, recordName):
        return self.ocx.dynamicCall("GetRepeatCnt(QString, QString)", [trCode, recordName])

    # Tran 데이터, 실시간 데이터, 체결잔고 데이터를 반환한다.
    # 1. Tran 데이터o
    # 2. 실시간 데이터
    # 3. 체결 데이터
    # 1. Tran 데이터
    # sJongmokCode : Tran명
    # sRealType : 사용안함
    # sFieldName : 레코드명
    # nIndex : 반복인덱스
    # sInnerFieldName: 아이템명
    # 2. 실시간 데이터
    # sJongmokCode : Key Code
    # sRealType : Real Type
    # sFieldName : Item Index (FID)
    # nIndex : 사용안함
    # sInnerFieldName:사용안함
    # 3. 체결 데이터
    # sJongmokCode : 체결구분
    # sRealType : “-1”
    # sFieldName : 사용안함
    # nIndex : ItemIndex
    # sInnerFieldName:사용안함
    @pyqtSlot(str, str, str, int, str, result=str)
    def CommGetData(self, jongmokCode, realType, fieldName, index, innerFieldName):
        return self.ocx.dynamicCall("CommGetData(QString, QString, QString, int, QString)",
                                    [jongmokCode, realType, fieldName, index, innerFieldName]).strip()

    # strRealType – 실시간 구분
    # nFid – 실시간 아이템
    # Ex) 현재가출력 - openApi.GetCommRealData(“주식시세”, 10);
    # 참고)실시간 현재가는 주식시세, 주식체결 등 다른 실시간타입(RealType)으로도 수신가능
    @pyqtSlot(str, int, result=str)
    def GetCommRealData(self, realType, fid):
        return self.ocx.dynamicCall("GetCommRealData(QString, int)", [realType, fid]).strip()

    # 주식 주문을 서버로 전송한다.
    # sRQName - 사용자 구분 요청 명
    # sScreenNo - 화면번호[4]
    # sAccNo - 계좌번호[10]
    # nOrderType - 주문유형 (1:신규매수, 2:신규매도, 3:매수취소, 4:매도취소, 5:매수정정, 6:매도정정)
    # sCode, - 주식종목코드
    # nQty – 주문수량
    # nPrice – 주문단가
    # sHogaGb - 거래구분
    # sHogaGb – 00:지정가, 03:시장가, 05:조건부지정가, 06:최유리지정가, 07:최우선지정가, 10:지정가IOC, 13:시장가IOC, 16:최유리IOC, 20:지정가FOK, 23:시장가FOK, 26:최유리FOK, 61:장전시간외종가, 62:시간외단일가, 81:장후시간외종가
    # ※ 시장가, 최유리지정가, 최우선지정가, 시장가IOC, 최유리IOC, 시장가FOK, 최유리FOK, 장전시간외, 장후시간외 주문시 주문가격을 입력하지 않습니다.
    # ex)
    # 지정가 매수 - openApi.SendOrder("RQ_1", "0101", "5015123410", 1, "000660", 10, 48500, "00", "");
    # 시장가 매수 - openApi.SendOrder("RQ_1", "0101", "5015123410", 1, "000660", 10, 0, "03", "");
    # 매수 정정 - openApi.SendOrder("RQ_1","0101", "5015123410", 5, "000660", 10, 49500, "00", "1");
    # 매수 취소 - openApi.SendOrder("RQ_1", "0101", "5015123410", 3, "000660", 10, "00", "2");
    # sOrgOrderNo – 원주문번호
    @pyqtSlot(str, str, str, int, str, int, int, str, str, result=int)
    def SendOrder(self, rQName, screenNo, accNo, orderType, code, qty, price, hogaGb, orgOrderNo):
        print("sendOrder", rQName, screenNo, accNo, orderType, code, qty, price, hogaGb, orgOrderNo)
        return self.ocx.dynamicCall("SendOrder(QString, QString, QString, int, QString, int, int, QString, QString)",
                                    [rQName, screenNo, accNo, orderType, code, qty, price, hogaGb, orgOrderNo])

    # 체결잔고 데이터를 반환한다.
    @pyqtSlot(int, result=str)
    def GetChejanData(self, fid):
        return self.ocx.dynamicCall("GetChejanData(int)", [fid])

    # 서버에 저장된 사용자 조건식을 가져온다.
    @pyqtSlot(result=int)
    def GetConditionLoad(self):
        res = self.ocx.dynamicCall("GetConditionLoad()")
        if res == 1:
            print('GetConditionLoad() success')
        else:
            print('GetConditionLoad() failed')

    # 조건검색 조건명 리스트를 받아온다.
    # 조건명 리스트(인덱스^조건명)
    # 조건명 리스트를 구분(“;”)하여 받아온다
    @pyqtSlot(result=str)
    def GetConditionNameList(self):
        return self.ocx.dynamicCall("GetConditionNameList()")

    # 조건검색 종목조회TR송신한다.
    # LPCTSTR strScrNo : 화면번호
    # LPCTSTR strConditionName : 조건명
    # int nIndex : 조건명인덱스
    # int nSearch : 조회구분(0:일반조회, 1:실시간조회, 2:연속조회)
    # 1:실시간조회의 화면 개수의 최대는 10개
    @pyqtSlot(str, str, int, int)
    @SyncRequestDecorator.kiwoom_sync_request
    def SendCondition(self, scrNo, conditionName, index, search):
        self.ocx.dynamicCall("SendCondition(QString,QString, int, int)", [scrNo, conditionName, index, search])

    # 실시간 조건검색을 중지합니다.
    # ※ 화면당 실시간 조건검색은 최대 10개로 제한되어 있어서 더 이상 실시간 조건검색을 원하지 않는 조건은 중지해야만 카운트 되지 않습니다.
    @pyqtSlot(str, str, int)
    def SendConditionStop(self, scrNo, conditionName, index):
        self.ocx.dynamicCall("SendConditionStop(QString, QString, int)", [scrNo, conditionName, index])

    # 복수종목조회 Tran을 서버로 송신한다.
    # OP_ERR_RQ_STRING – 요청 전문 작성 실패
    # OP_ERR_NONE - 정상처리
    #
    # sArrCode – 종목간 구분은 ‘;’이다.
    # nTypeFlag – 0:주식관심종목정보, 3:선물옵션관심종목정보
    @pyqtSlot(str, bool, int, int, str, str)
    @SyncRequestDecorator.kiwoom_sync_request
    def CommKwRqData(self, arrCode, next, codeCount, typeFlag, rQName, screenNo):
        self.ocx.dynamicCall("CommKwRqData(QString, QBoolean, int, int, QString, QString)",
                             [arrCode, next, codeCount, typeFlag, rQName, screenNo])

    # 실시간 등록을 한다.
    # strScreenNo : 화면번호
    # strCodeList : 종목코드리스트(ex: 039490;005930;…)
    # strFidList : FID번호(ex:9001;10;13;…)
    #   9001 – 종목코드
    #   10 - 현재가
    #   13 - 누적거래량
    # strOptType : 타입(“0”, “1”)
    # 타입 “0”은 항상 마지막에 등록한 종목들만 실시간등록이 됩니다.
    # 타입 “1”은 이전에 실시간 등록한 종목들과 함께 실시간을 받고 싶은 종목을 추가로 등록할 때 사용합니다.
    # ※ 종목, FID는 각각 한번에 실시간 등록 할 수 있는 개수는 100개 입니다.
    @pyqtSlot(str, str, str, int, result=int)
    def SetRealReg(self, screenNo, codeList, fidList, optType):
        return self.ocx.dynamicCall("SetRealReg(QString, QString, QString, QString)",
                                    [screenNo, codeList, fidList, optType])

    # 종목별 실시간 해제
    # strScrNo : 화면번호
    # strDelCode : 실시간 해제할 종목코드
    # -화면별 실시간해제
    # 여러 화면번호로 걸린 실시간을 해제하려면 파라메터의 화면번호와 종목코드에 “ALL”로 입력하여 호출하시면 됩니다.
    # SetRealRemove(“ALL”, “ALL”);
    # 개별화면별로 실시간 해제 하시려면 파라메터에서 화면번호는 실시간해제할
    # 화면번호와 종목코드에는 “ALL”로 해주시면 됩니다.
    # SetRealRemove(“0001”, “ALL”);
    # -화면의 종목별 실시간해제
    # 화면의 종목별로 실시간 해제하려면 파라메터에 해당화면번호와 해제할
    # 종목코드를 입력하시면 됩니다.
    # SetRealRemove(“0001”, “039490”);
    @pyqtSlot(str, str)
    def SetRealRemove(self, scrNo, delCode):
        self.ocx.dynamicCall("SetRealRemove(QString, QString)", [scrNo, delCode])

    # 차트 조회한 데이터 전부를 배열로 받아온다.
    # LPCTSTR strTrCode : 조회한TR코드
    # LPCTSTR strRecordName: 조회한 TR명
    # ※항목의 위치는 KOA Studio의 TR목록 순서로 데이터를 가져옵니다.
    # 예로 OPT10080을 살펴보면 OUTPUT의 멀티데이터의 항목처럼 현재가, 거래량, 체결시간등 순으로 항목의 위치가 0부터 1씩 증가합니다.
    @pyqtSlot(str, str, result=str)
    def GetCommDataEx(self, trCode, recordName):
        return dumps(self.ocx.dynamicCall("GetCommDataEx(QString, QString)", [trCode, recordName]))

    # 차트의 특정 조회데이터를 받아온다.
    @pyqtSlot(str, str, int, str, result=str)
    def GetCommData(self, trCode, recordName, nIndex, itemName):
        return self.ocx.dynamicCall("GetCommData(QString, QString, int, QString)",
                                    [trCode, recordName, nIndex, itemName])

    # 리얼 시세를 끊는다.
    # 화면 내 모든 리얼데이터 요청을 제거한다.
    # 화면을 종료할 때 반드시 위 함수를 호출해야 한다.
    # Ex) openApi.DisconnectRealData(“0101”);
    @pyqtSlot(str)
    def DisconnectRealData(self, scnNo):
        self.ocx.dynamicCall("DisconnectRealData(QString)", [scnNo])

    # 종목코드의 한글명을 반환한다.
    # strCode – 종목코드
    # 종목한글명
    @pyqtSlot(str, result=str)
    def GetMasterCodeName(self, code):
        return self.ocx.dynamicCall("GetMasterCodeName(QString)", [code])

    # 국내 주식 시장별 종목코드를 ;로 구분하여 전달
    # strMarket – 종목코드
    # 마켓 구분값
    # 0 : 장내
    # 10 : 코스닥
    # 3 : ELW
    # 8 : ETF
    # 50 : KONEX
    # ...
    @pyqtSlot(str, result=str)
    def GetCodeListByMarket(self, strMarket):
        return self.ocx.dynamicCall("GetCodeListByMarket(QString)", [strMarket])

    # 입력한 종목의 전일가를 전달
    # strCode – 종목코드
    def GetMasterLastPrice(self, code):
        return self.ocx.dynamicCall("GetMasterLastPrice(QString)", [code])

    # 통신 연결 상태 변경시 이벤트
    # nErrCode가 0이면 로그인 성공, 음수면 실패
    def OnEventConnect(self, errCode):
        if errCode == 0:
            print('로그인 성공!')
        else:
            print('Error')
            kill(getpid(), 9)

    # 수신 메시지 이벤트
    def OnReceiveMsg(self, scrNo, rQName, trCode, msg):
        print('_OnReceiveMsg()', scrNo, rQName, trCode, msg)

    # 실시간 시세 이벤트
    def OnReceiveRealData(self, jongmokCode, realType, realData):
        # print('_OnReceiveRealData', jongmokCode, realType, realData)
        pass

    # 체결데이터를 받은 시점을 알려준다.
    # sGubun – 0:주문체결통보, 1:잔고통보, 3:특이신호
    # sFidList – 데이터 구분은 ‘;’ 이다.
    def OnReceiveChejanData(self, gubun, itemCnt, fidList):
        # print('_OnReceiveChejanData()', gubun, itemCnt, fidList)
        pass

    # 로컬에 사용자조건식 저장 성공여부 응답 이벤트
    def OnReceiveConditionVer(self, ret, msg):
        print('_OnReceiveConditionVer()', ret, msg)

    # 편입, 이탈 종목이 실시간으로 들어옵니다.
    # strCode : 종목코드
    # strType : 편입(“I”), 이탈(“D”)
    # strConditionName : 조건명
    # strConditionIndex : 조건명 인덱스
    def OnReceiveRealCondition(self, code, strType, conditionName, conditionIndex):
        print('_OnReceiveRealCondition()', code, strType, conditionName, conditionIndex)

    @SyncRequestDecorator.kiwoom_sync_callback
    def OnReceiveTrCondition(self, scrNo, codeList, conditionName, index, next, **kwargs):
        print('_OnReceiveTrCondition()', scrNo, codeList, conditionName, index, next)

    # Tran 수신시 이벤트
    @SyncRequestDecorator.kiwoom_sync_callback
    def OnReceiveTrData(self, scrNo, rQName, trCode, recordName, prevNext, dataLength, errorCode, message, splmMsg,
                        **kwargs):
        print('OnReceiveTrData()', scrNo, rQName, trCode, recordName, prevNext, dataLength, errorCode, message,
              splmMsg)

class SpartaQuant(QMainWindow):
    def __init__(self):
        super().__init__()
        self._kiwoom = Kiwoom()

        t1 = Thread(target=self.main_thread)
        t1.daemon = True
        t1.start()

    def main_thread(self):
        ###############################
        # 1. 로그인                    #
        ###############################

        # 로그인 시도
        self._kiwoom.CommConnect()

        # 로그인 완료 대기
        while True:
            if self._kiwoom.GetLoginInfo("ACCOUNT_CNT") != "":
                break
            print("로그인 대기 중...")
            sleep(5)
        sleep(5)


if __name__ == "__main__":
    app = QApplication(argv)
    myWindow = SpartaQuant()
    trader = myWindow._kiwoom
    app.exec_()