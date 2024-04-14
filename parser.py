#!/usr/bin/env python
import argparse
import glom
import logging
import orjson
import re
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from elasticsearch import Elasticsearch, ConflictError, NotFoundError
from pisicap import api, utils
from rich import progress as prog
from tenacity import retry_if_result, stop_after_attempt, wait_fixed


class customSICAP(api.SICAP):
    retry_rules = dict(
        retry=retry_if_result(utils.not_200),
        stop=stop_after_attempt(5),
        wait=wait_fixed(5),
    )


class Parser:
    def __init__(self, user_options: dict = {}) -> None:
        self.opt = user_options
        self.es_client = Elasticsearch(self.opt["es_host"] or os.environ["ES_HOST"])
        self.log = self._make_logger()
        self.api = customSICAP(secure=self.opt["secure"], verbose=self.opt["verbose"])

    def main(self):
        raise NotImplementedError()

    @staticmethod
    def _string_to_int(text: str) -> str:
        digits = re.sub(r"\D", "", text, flags=re.IGNORECASE)
        return int(digits) if digits else None

    def _clean_fiscal_code(self, code: str) -> int:
        if not isinstance(code, str):
            code = "0"

        return self._string_to_int(code) or 0

    def _multithread_run(self, task, args_iter):
        results = []

        with ThreadPoolExecutor(max_workers=self.opt["threads"]) as executor:
            with prog.Progress(
                prog.BarColumn(),
                prog.TaskProgressColumn(),
                prog.TimeRemainingColumn(),
                prog.MofNCompleteColumn(),
            ) as prog_bar:
                futures = [executor.submit(task, i) for i in args_iter]
                task = prog_bar.add_task("👾", total=len(futures))

                for f in as_completed(futures):
                    try:
                        results.append(f.result())
                    except Exception as e:
                        self.log.error(e, exc_info=True)
                    finally:
                        prog_bar.advance(task)

        return results

    def _upsert_es_doc(self, doc, doc_id, index=None):
        return self.es_client.update(
            index=(index or self.opt["es_index"]),
            id=doc_id,
            doc=doc,
            doc_as_upsert=True,
        )

    def _make_logger(self):
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

    def _clean_entity(self, entity: dict) -> None:
        for key in [
            "countryId",
            "cityItem",
            "numericFiscalNumber",
            "address",
            "phone",
            "fax",
            "email",
            "url",
            "country",
            "postalCode",
            "bankAccount",
            "bankName",
            "sysEntityTypeId",
            "sysNoticeEntityAddressType",
        ]:
            glom.delete(entity, key, ignore_missing=True)

        glom.assign(
            entity,
            "fiscalNumberInt",
            self._clean_fiscal_code(entity.get("fiscalNumber")),
        )

    def get_ca_entity(self, entity_id: int) -> dict:
        if cached := self._get_entity_from_es("ca", entity_id):
            return cached

        response = self.api.getCAEntityView(entity_id)
        entity = orjson.loads(response.text)
        self._clean_entity(entity)
        self._insert_entity_to_es(entity, "ca", entity_id)
        return entity

    def get_su_entity(self, entity_id: int) -> dict:
        if cached := self._get_entity_from_es("su", entity_id):
            return cached

        response = self.api.getSUEntityView(entity_id)
        entity = orjson.loads(response.text)
        self._clean_entity(entity)
        self._insert_entity_to_es(entity, "su", entity_id)
        return entity

    def _insert_entity_to_es(self, entity, ent_type, ent_id):
        # keeping the original index names for compatibility
        indexes = {"ca": "autoritati", "su": "firme"}
        doc = {"data": entity, "isNew": True, "updatedAt": utils.now()}
        try:
            self._upsert_es_doc(doc, ent_id, indexes[ent_type])
        except ConflictError:
            # occurs when updating same doc by multiple threads
            pass

    def _get_entity_from_es(self, ent_type: str, ent_id):
        indexes = {"ca": "autoritati", "su": "firme"}
        resp = None
        try:
            raw = dict(self.es_client.get(index=indexes[ent_type], id=ent_id))
            resp = glom.glom(raw, "_source.data")
        finally:
            return resp


# CAN stands for Contract Award Notices
class ParserCAN(Parser):
    def __init__(self, user_options: dict = {}) -> None:
        if "es_index" not in user_options:
            user_options["es_index"] = "licitatii-publice"

        super().__init__(user_options)
        self.log.info("Contract Award Notices mode")

    def get_notices_list(self) -> dict:
        options = {"pageSize": 3000}

        if self.opt.get("date"):
            options["startPublicationDate"] = utils.date_parsed(self.opt["date"])
        if self.opt.get("end_date"):
            options["startPublicationDate"] = utils.date_parsed(self.opt["end_date"])

        response = self.api.getCANoticeList(options)
        notices_list = orjson.loads(response.text)
        assert notices_list["searchTooLong"] is False
        return notices_list

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
        item["nationalId"] = self._clean_fiscal_code(authority)

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
            notice, f"{path}Int", self._clean_fiscal_code(glom.glom(notice, path))
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
            if not g:
                continue

            glom.assign(
                g, "fiscalNumberInt", self._clean_fiscal_code(g.get("fiscalNumber"))
            )

            if ga := glom.glom(g, "address"):
                for path in wkeys:
                    glom.delete(ga, path, ignore_missing=True)

                glom.assign(
                    ga,
                    "nationalIDNumberInt",
                    self._clean_fiscal_code(ga.get("nationalIDNumber")),
                )

    def main(self) -> None:
        return self.parse_notices()

    def parse_notices(self):
        notices_list = self.get_notices_list()
        items = list(map(self.__clean_item, notices_list["items"]))
        self.log.info(f"Total notices to fetch: {notices_list['total']}")
        return self._multithread_run(self.add_notice, items)

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
        return self._upsert_es_doc(es_doc, item["caNoticeId"])


# Unawarded Contract Notices
class ParserUCA(Parser):
    def __init__(self, user_options: dict = {}) -> None:
        if "es_index" not in user_options:
            user_options["es_index"] = "licitatii-deschise"

        super().__init__(user_options)
        self.log.info("Unawarded Contract Notices mode")

    def main(self):
        self.parse_notices()

    def parse_notices(self):
        notices_ids = set([n["procedureId"] for n in self.get_notices_list()])
        cached_unawarded_ids = self.get_cached_unawarded()

        return self._multithread_run(
            self.add_notice, notices_ids | cached_unawarded_ids
        )

    def add_notice(self, notice_id: int):
        if not (notice := self.get_notice(notice_id)):
            return

        time_now = utils.now()
        notice_clean = {"procedureId": notice_id, "added": time_now}

        for key in [
            "typeNoticeId",
            "header.contractName",
            "header.sysNoticeType.id",
            "header.noticePublicationDate",
            "header.estimatedValue",
            "header.contractingAuthorityName",
            "header.contractingAuthorityId",
            "phaseInfo.sysProcedurePhaseId",
        ]:
            notice_clean[key] = glom.glom(notice, key, default=None)

        suppliers = []
        modified = False

        participants = self.get_suppliers(notice_id)

        for part in participants:
            entity = self.get_su_entity(part['entityId']) if part['entityId'] else self._missing_entity_fallback(part)
            entity["isLead"] = part.get("isLead", False)
            entity['statementSupplierId'] = part['statementSupplierId']
            entity["participantState"] = glom.glom(part, "sysProcedureSupplierClass.text", default="N/A")
            entity["date_added"] = time_now
            suppliers.append(entity)

        if cached := self.get_cached_notice(notice_id):
            if (
                cached["notice"]["phaseInfo.sysProcedurePhaseId"]
                != notice_clean["phaseInfo.sysProcedurePhaseId"]
            ):
                modified = True

            cached["notice"].update(notice_clean)
            cached_supp = [s["entityId"] for s in cached["suppliers"]]

            for supp in suppliers:
                if supp["entityId"] not in cached_supp:
                    cached["suppliers"].append(supp)
                    modified = True

            es_doc = cached
        else:
            ca_entity = self.get_ca_entity(
                notice_clean["header.contractingAuthorityId"]
            )

            es_doc = {
                "notice": notice_clean,
                "authority": ca_entity,
                "suppliers": suppliers,
            }

        if modified or es_doc["notice"].get("modified") is None:
            es_doc["notice"]["modified"] = time_now

        return self._upsert_es_doc(es_doc, notice_id)

    def get_notice(self, notice_id: int) -> dict:
        response = self.api.getProcedureView(notice_id)
        notice = orjson.loads(response.text)
        return notice

    def get_notices_list(self):
        start_time = utils.date_parsed(self.opt.get("date") or utils.yesterday(), True)
        end_time = utils.date_parsed(self.opt.get("end_date") or str(utils.now()), True)
        tmp_time = start_time + utils.DELTA_6MONTHS

        options = {
            "sysNoticeTypeIds": [2, 6, 7, 17],
            "pageSize": 5000,
            "sysProcedureStateId": 2,
            "startPublicationDate": utils.date_iso(start_time),
            "endPublicationDate": utils.date_iso(end_time),
        }

        if tmp_time < end_time:
            options["endPublicationDate"] = utils.date_iso(tmp_time)

        items = []

        while True:
            response = self.api.getCNoticeList(options)
            notices = orjson.loads(response.text)
            assert notices["searchTooLong"] is False

            items += notices["items"]

            if tmp_time >= end_time:
                break

            options["startPublicationDate"] = options["endPublicationDate"]
            tmp_time += utils.DELTA_6MONTHS
            options["endPublicationDate"] = utils.date_iso(tmp_time)

        return items

    def get_cached_notice(self, notice_id: int) -> dict | None:
        try:
            resp = self.es_client.get(index=self.opt["es_index"], id=notice_id)
            return resp["_source"]
        except NotFoundError:
            pass

    def get_suppliers(self, notice_id) -> list:
        rfq_resp = self.api.getRfqInvitationView(notice_id)
        # When access to suppliers is denied
        if rfq_resp.status_code == 400:
            self.log.warning(f'Suppliers denied for UCA notice {notice_id}')
            return []

        rfq_inv = orjson.loads(rfq_resp.text)
        proc_id = rfq_inv['procedureId'] if rfq_inv else notice_id
        proc_rep = orjson.loads(self.api.getProcedureReports(proc_id).text)
        statement_docs = []

        for doc in proc_rep["items"]:
            if doc["sysNoticeDocumentType"]["id"] == 10:
                statement_docs.append(doc["procedureStatementId"])

        if not statement_docs:
            return []

        proc_stat = orjson.loads(
            self.api.getProcedureStatementView(statement_docs[0]).text
        )

        suppliers = []
        for p in proc_stat["statementSuppliers"]:
            suppliers.extend(p['statementParticipants'])

        return suppliers

    def get_cached_unawarded(self) -> list:
        """{ 2: "Depunere ofertă", 3: "Evaluare calificare si tehnica", 11: "Evaluare financiara", 4: "Deliberare", 5: "Atribuita" }"""
        try:
            resp = self.es_client.search(
                index=self.opt["es_index"],
                stored_fields="_id",
                size=10000,
                query={
                    "bool": {
                        "must_not": {"match": {"notice.phaseInfo.sysProcedurePhaseId": 5}}
                    }
                },
            )
        except NotFoundError:
            return set()

        return set([int(hit["_id"]) for hit in resp["hits"]["hits"]])

    def _missing_entity_fallback(self, participant):
        return {
            'entityId': None,
            'fiscalNumberInt': self._clean_fiscal_code(participant['fiscalNumber']),
            'entityName': participant['name'],
            'city': "",
            'county': "",
            'fiscalNumber': participant['fiscalNumber'],
        }

# Direct Aquisitions Award Notices
class ParserDAAN(Parser):
    def __init__(self, user_options: dict = {}) -> None:
        if "es_index" not in user_options:
            user_options["es_index"] = "achizitii-offline"

        super().__init__(user_options)
        self.log.info("Direct Aquisitions Award Notices mode")

    def main(self):
        self.parse_notices()

    def parse_notices(self):
        notices = self.get_notices_list()
        self.log.info(f"Total notices to fetch: {len(notices)}")
        return self._multithread_run(self.add_notice, notices)

    def add_notice(self, notice_id: int):
        notice = self.get_notice(notice_id)
        ca_entity = self.get_ca_entity(notice["contractingAuthorityID"])
        su_entity = (
            self.get_su_entity(notice["supplierID"])
            if notice["supplierID"]
            else self.su_entity_fallback(notice["noticeEntityAddress"])
        )
        self.__clean_notice(notice)

        es_doc = {
            "notice": notice,
            "authority": ca_entity,
            "supplier": su_entity,
        }
        return self._upsert_es_doc(es_doc, notice_id)

    def get_notices_list(self) -> set:
        options = {"pageSize": 2000}

        start_date = utils.date_parsed(self.opt.get("date") or utils.yesterday(), 1)
        end_date = (
            utils.date_parsed(self.opt["end_date"], 1)
            if self.opt.get("end_date")
            else utils.now()
        )
        tmp_date = start_date
        items = set()

        while end_date > tmp_date:
            options["publicationDateStart"] = utils.date_iso(tmp_date)
            tmp_date += utils.DELTA_1H
            options["publicationDateEnd"] = utils.date_iso(tmp_date)

            response = self.api.getDaAwardNoticeList(options)
            notices = orjson.loads(response.text)

            items.update(i["daAwardNoticeId"] for i in notices["items"])

        return items

    def get_notice(self, notice_id: int) -> dict:
        response = self.api.getPublicDAAwardNotice(notice_id)
        notice = orjson.loads(response.text)
        return notice

    def su_entity_fallback(self, eaddr: dict) -> dict:
        return {
            "city": eaddr["city"],
            "county": None,
            "entityId": eaddr["noticeEntityAddressID"],
            "entityName": eaddr["organization"],
            "fiscalNumber": eaddr["fiscalNumber"],
            "fiscalNumberInt": self._clean_fiscal_code(eaddr["fiscalNumber"]),
        }

    def __clean_notice(self, notice: dict) -> None:
        glom.delete(notice, "noticeEntityAddress", ignore_missing=True)


# Direct Aquisitions
class ParserDA(Parser):
    def __init__(self, user_options: dict = {}) -> None:
        if "es_index" not in user_options:
            user_options["es_index"] = "achizitii-directe"

        super().__init__(user_options)
        self.log.info("Direct Aquisitions mode")

    def main(self):
        return self.parse_notices()

    def parse_notices(self):
        items = self.get_notices_list()
        return self._multithread_run(self.add_direct_acquisition, items)

    def get_notices_list(self) -> list:
        options = {}

        if self.opt.get("date"):
            options["finalizationDateStart"] = utils.date_parsed(self.opt["date"])
        if self.opt.get("end_date"):
            options["finalizationDateEnd"] = utils.date_parsed(self.opt["end_date"])

        def notices_per_cpv(cpv):
            opt = options.copy()
            opt["cpvCodeId"] = cpv
            response = self.api.getDirectAcquisitionList(opt)
            notices = orjson.loads(response.text)
            if notices["searchTooLong"]:
                self.log.warning("DA [searchTooLong]: CPV {cpv}")

            return notices["items"]

        notices = []
        for item_list in self._multithread_run(notices_per_cpv, self.api.cpvs().keys()):
            notices.extend(item_list)

        return notices

    def add_direct_acquisition(self, item):
        da = self.get_da(item["directAcquisitionId"])
        self.__clean_da(da)
        authority = self.get_ca_entity(da["contractingAuthorityID"])
        supplier = self.get_su_entity(da["supplierId"])

        es_doc = {
            "item": item,
            "publicDirectAcquisition": da,
            "authority": authority,
            "supplier": supplier,
            "istoric": False,
        }
        return self._upsert_es_doc(es_doc, item["directAcquisitionId"])

    def get_da(self, notice_id):
        response = self.api.getPublicDirectAcquisition(notice_id)
        return orjson.loads(response.text)

    def __clean_da(self, da) -> None:
        for item in glom.glom(da, "directAcquisitionItems"):
            glom.delete(item, "assignedUserEmail", ignore_missing=True)


def parse_cli_args():
    parser = argparse.ArgumentParser(
        prog="SICAP Parser", description="Fetch data from e-licitatie.ro"
    )

    parser.add_argument("-v", "--verbose", action="store_true", default=False)
    parser.add_argument("-s", "--secure", action="store_true", default=True)
    parser.add_argument("-d", "--date", help="YYYY-MM-DD, defaults to yesterday")
    parser.add_argument("-e", "--end-date", help="YYYY-MM-DD, defaults to now")
    parser.add_argument(
        "-u", "--es-host", help="Elasticsearch URL, can be also set via ES_HOST env var"
    )
    parser.add_argument("-t", "--threads", type=int, default=5)
    parser.add_argument(
        "-m",
        "--mode",
        required=True,
        choices=["CAN", "UCA", "DAAN", "DA"],
        help="""
        CAN (Contract Award Notices)
        UCA (Unawarded Contract Notices)
        DAAN (Direct Aquisitions Award Notices / Offline)
        DA (Direct Aquisitions)
        """,
    )

    args = parser.parse_args()
    return vars(args)


def main():
    args = parse_cli_args()
    parser_class = globals()[f"Parser{args['mode']}"]
    parser = parser_class(args)
    parser.main()


if __name__ == "__main__":
    """TODO:
    - move the classes to separate files
    - add option to manually pass notices id (via an input file)
    - add api key auth for elastic
    """
    main()
