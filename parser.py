#!/usr/bin/env python
import glom
import logging
import orjson
import pisicap
import re
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from elasticsearch import Elasticsearch
from rich import progress as prog


class Parser:
    def __init__(self, user_options: dict = {}) -> None:
        self.opt = {"verbose": False, "secure": True, "threads": 5}
        self.opt.update(user_options)
        self.es_client = Elasticsearch(self.opt["es_host"])
        self.log = self.__make_logger()
        self.api = pisicap.api.SICAP(
            secure=self.opt["secure"], verbose=self.opt["verbose"]
        )

    def get_notices_list(self) -> dict:
        options = { "pageSize": 3000 }
        if self.opt.get('startDate'):
            options["startPublicationDate"] = self.opt['startDate']
        if self.opt.get('endDate'):
            options["endPublicationDate"] = self.opt['endDate']

        response = self.api.getCNoticeList(options)
        return orjson.loads(response.text)

    def get_notice(self, id_anunt: int) -> dict:
        response = self.api.getCANotice(id_anunt)
        return orjson.loads(response.text)

    def get_contracts(self, id_anunt: int) -> list[dict]:
        response = self.api.getCANoticeContracts({"caNoticeId": id_anunt})
        return orjson.loads(response.text)

    def __clean_item(self, item: dict) -> dict:
        for key in [
            "errataNo",
            "estimatedValueExport",
            "highestOfferValue",
            "isOnline",
            "lowestOfferValue",
            "maxTenderReceiptDeadline",
            "minTenderReceiptDeadline",
            "sysNoticeVersionId",
            "tenderReceiptDeadlineExport",
            "versionNo",
        ]:
            glom.delete(item, key, ignore_missing=True)

        item["cpvCode"] = item["cpvCodeAndName"].split(" - ")[0]
        authority = item["contractingAuthorityNameAndFN"].split("-")[0]
        item["nationalId"] = self.__clean_fiscal_code(authority)

        return item

    def __clean_notice(self, notice: dict) -> None:
        u = "_U" if notice.get("isUtilityContract") else ""

        for path in [
            "acAssignedUser",
            "ackDocs",
            "ackDocsCount",
            "actions",
            "cNoticeId",
            "conditions",
            "createDate",
            "errorList",
            "hasErrors",
            "initState",
            "isCA",
            "isCompleting",
            "isCorrecting",
            "isLeProcedure",
            "isModifNotice",
            "isOnlineProcedure",
            "isView",
            "legislationType",
            "paapModel",
            "paapSpentValue",
            "parentCaNoticeId",
            "parentSysNoticeVersionId",
            "sentToJOUE",
            "tedNoticeNo",
            "versionNumber",
            "caNoticeEdit_New" if u else "caNoticeEdit_New_U",
            f"caNoticeEdit_New{u}.section0_New",
            f"caNoticeEdit_New{u}.annexD_New{u}",
            f"caNoticeEdit_New{u}.section1_New{u}.section1_1.caAddress.attentionTo",
            f"caNoticeEdit_New{u}.section1_New{u}.section1_1.caAddress.contactPerson",
            f"caNoticeEdit_New{u}.section1_New{u}.section1_1.caAddress.contactPoints",
            f"caNoticeEdit_New{u}.section1_New{u}.section1_1.caAddress.electronicDocumentsSendingUrl",
            f"caNoticeEdit_New{u}.section1_New{u}.section1_1.caAddress.electronicDocumentsSendingUrl",
            f"caNoticeEdit_New{u}.section1_New{u}.section1_1.caAddress.email",
            f"caNoticeEdit_New{u}.section1_New{u}.section1_1.caAddress.fax",
            f"caNoticeEdit_New{u}.section1_New{u}.section1_1.caAddress.nutsCode",
            f"caNoticeEdit_New{u}.section1_New{u}.section1_1.caAddress.nutsCodeID",
            f"caNoticeEdit_New{u}.section1_New{u}.section1_1.caAddress.phone",
            f"caNoticeEdit_New{u}.section1_New{u}.section1_1.caNoticeId",
            f"caNoticeEdit_New{u}.section1_New{u}.section1_1.canEdit",
            f"caNoticeEdit_New{u}.section1_New{u}.section1_1.noticePreviousPublication",
            f"caNoticeEdit_New{u}.section1_New{u}.section1_1.sectionName",
            f"caNoticeEdit_New{u}.section1_New{u}.section1_1.sectionCode",
            f"caNoticeEdit_New{u}.section1_New{u}.section1_2_New",
            f"caNoticeEdit_New{u}.section1_New{u}.section1_4_New",
            f"caNoticeEdit_New{u}.section1_New{u}.section1_5",
            f"caNoticeEdit_New{u}.section2_New{u}.section2_1_New{u}.caNoticeId",
            f"caNoticeEdit_New{u}.section2_New{u}.section2_1_New{u}.canEdit",
            f"caNoticeEdit_New{u}.section2_New{u}.section2_1_New{u}.noticePreviousPublication",
            f"caNoticeEdit_New{u}.section2_New{u}.section2_1_New{u}.sectionCode",
            f"caNoticeEdit_New{u}.section2_New{u}.section2_1_New{u}.sectionName",
            f"caNoticeEdit_New{u}.section2_New{u}.section2_1_New{u}.shouldShowSection217",
            f"caNoticeEdit_New{u}.section2_New{u}.section2_2_New{u}.caNoticeId",
            f"caNoticeEdit_New{u}.section2_New{u}.section2_2_New{u}.canEdit",
            f"caNoticeEdit_New{u}.section2_New{u}.section2_2_New{u}.noticePreviousPublication",
            f"caNoticeEdit_New{u}.section2_New{u}.section2_2_New{u}.previousPublication",
            f"caNoticeEdit_New{u}.section2_New{u}.section2_2_New{u}.sectionCode",
            f"caNoticeEdit_New{u}.section2_New{u}.section2_2_New{u}.sectionName",
            f"caNoticeEdit_New{u}.section2_New{u}.section2_2_New{u}.showPublishingAgreedSection",
            f"caNoticeEdit_New{u}.section4_New",
            f"caNoticeEdit_New{u}.section5",
            f"caNoticeEdit_New{u}.section6_New",
        ]:
            glom.delete(notice, path, ignore_missing=True)

        for path in [
            "communityProgramReference",
            "hasOptions",
            "noticeAwardCriteriaList",
            "optionsDescription",
            "sysEuropeanFund",
            "sysEuropeanFundId",
            "sysFinancingTypeId",
        ]:
            for lot in glom.glom(
                notice,
                f"caNoticeEdit_New{u}.section2_New{u}.section2_2_New{u}.descriptionList",
            ):
                glom.delete(lot, path, ignore_missing=True)

        path = (
            f"caNoticeEdit_New{u}.section1_New{u}.section1_1.caAddress.nationalIDNumber"
        )
        glom.assign(
            notice, f"{path}Int", self.__clean_fiscal_code(glom.glom(notice, path))
        )

    def __clean_contract(self, contract: dict) -> None:
        for path in [
            "actions",
            "conditions",
            "hasModifiedVersions",
            "modifiedCount",
        ]:
            glom.delete(contract, path)

        wkeys = [
            "attentionTo",
            "contactPerson",
            "contactPoints",
            "email",
            "fax",
            "nutsCode",
            "phone",
        ]

        # 😳
        for g in [
            glom.glom(contract, "winner"),
            *(glom.glom(contract, "winners") or []),
        ]:
            glom.assign(
                g, "fiscalNumberInt", self.__clean_fiscal_code(g.get("fiscalNumber"))
            )

            if ga := glom.glom(g, "address"):
                for path in wkeys:
                    glom.delete(ga, path, ignore_missing=True)

                glom.assign(
                    ga,
                    "nationalIDNumberInt",
                    self.__clean_fiscal_code(ga.get("nationalIDNumber")),
                )

    @staticmethod
    def __string_to_int(text: str) -> str:
        return re.sub(r"\D", "", text, flags=re.IGNORECASE)

    def __clean_fiscal_code(self, code: str) -> int:
        if not isinstance(code, str):
            code = "0"

        return int(self.__string_to_int(code))

    def main(self) -> None:
        results = self.parse_notices()
        breakpoint
        return results

    def parse_notices(self):
        notices_list = self.get_notices_list()
        items = list(map(self.__clean_item, notices_list["items"]))
        self.log.info(f"Total notices to fetch: {notices_list['total']}")
        return self.__multithread_run(self.add_notice, items)

    def add_notice(self, item: dict):
        notice = self.get_notice(item["caNoticeId"])
        self.__clean_notice(notice)
        contracts = self.get_contracts(item["caNoticeId"])

        for c in contracts["items"]:
            self.__clean_contract(c)

        es_doc = {
            "item": item,
            "publicNotice": notice,
            "noticeContracts": contracts,
            "istoric": False,
        }
        return self.__upsert_es_doc(es_doc, item["caNoticeId"])

    def __multithread_run(self, task, args_iter):
        with ThreadPoolExecutor(max_workers=self.opt["threads"]) as executor:
            with prog.Progress(
                prog.BarColumn(),
                prog.TaskProgressColumn(),
                prog.TimeRemainingColumn(),
                prog.MofNCompleteColumn(),
            ) as prog_bar:
                futures = [executor.submit(task, i) for i in args_iter]
                task = prog_bar.add_task("👾", total=len(futures))

                results = []

                for f in as_completed(futures):
                    try:
                        results.append(f.result())
                    except Exception as e:
                        self.log.error(e, exc_info=True)
                    finally:
                        prog_bar.advance(task)

                return results

    def __upsert_es_doc(self, doc, doc_id):
        return self.es_client.update(
            index=self.opt["es_index"],
            id=doc_id,
            doc=doc,
            doc_as_upsert=True,
        )

    def __make_logger(self):
        lformat = "%(asctime)s - %(levelname)s - %(message)s"
        formatter = logging.Formatter(lformat)
        logging.basicConfig(format=lformat)

        logger = logging.getLogger("pisicap-parser")
        logger.setLevel(logging.DEBUG if self.opt["verbose"] else logging.INFO)

        file_handler = logging.FileHandler("errors.log")
        file_handler.setLevel(logging.ERROR)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        return logger


if __name__ == "__main__":
    options = {
        "es_host": os.environ["ES_HOST"],
        "es_index": "licitatii-publice",
        "startDate": "2024-01-09T00:00:00.000Z"
    }
    parser = Parser(options)
    parser.main()
