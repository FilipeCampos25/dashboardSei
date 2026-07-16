from __future__ import annotations

import csv
import hashlib
import json
import shutil
import unittest
from pathlib import Path

from app.services.act_shadow_scoring import (
    SHADOW_SCORING_VERSION,
    comparison_text,
    export_shadow_report,
    instrument_number_key,
    process_number_key,
    score_shadow_candidate,
)
from app.services.act_normalizer import export_normalized_csv


class ACTShadowScoringTests(unittest.TestCase):
    def test_comparison_keys_tolerate_case_accents_mojibake_and_nup_variant(self) -> None:
        self.assertEqual(comparison_text("COOPERAÇÃO TÉCNICA"), "cooperacao tecnica")
        self.assertEqual(comparison_text("CooperaÃ§Ã£o TÃ©cnica"), "cooperacao tecnica")
        self.assertEqual(instrument_number_key("ACT nº 04 / 20"), "4/2020")
        self.assertEqual(process_number_key("60093.00015/2020-60"), "60093000015202060")
        self.assertEqual(process_number_key("60093.000015/2020-60"), "60093000015202060")

    def test_body_mentions_do_not_penalize_shadow_candidate(self) -> None:
        record = self._record(
            title="Acordo de Cooperação Técnica 04/2020",
            text=self._act_text("04/2020", "60093.000015/2020-60", body="Portaria de nomeação. O plano de trabalho integra o acordo."),
            numero="04/2020",
            alignment="aligned",
        )
        score = score_shadow_candidate(record, {"numero_act": "04/2020"})
        rules = {item["rule"] for item in score["contributions"]}
        self.assertTrue(score["eligible"])
        self.assertIn("preview_number_compatible", rules)
        self.assertFalse(any("penalty_portaria" in rule or "penalty_plano" in rule for rule in rules))

    def test_report_minuta_and_despacho_are_ineligible(self) -> None:
        cases = (
            ("Relatório 48 - Encerramento de ACT", True),
            ("Minuta de Acordo de Cooperação Técnica", False),
            ("Despacho sobre Acordo de Cooperação Técnica", False),
        )
        for title, report in cases:
            with self.subTest(title=title):
                record = self._record(title=title, text=self._act_text("04/2020", "60093.000015/2020-60"), numero="04/2020", alignment="aligned")
                record["relatorio_encerramento"] = report
                score = score_shadow_candidate(record, {"numero_act": "04/2020"})
                self.assertFalse(score["eligible"])
                self.assertTrue(score["gates"])

    def test_external_agreement_without_censipam_context_is_ineligible(self) -> None:
        record = self._record(
            title="Acordo de Cooperação Técnica 109/2022",
            text=self._act_text("109/2022", "14022.172688/2022-07"),
            numero="109/2022",
            alignment="unknown",
        )
        record["has_internal_context"] = False
        score = score_shadow_candidate(record, {})
        self.assertFalse(score["eligible"])
        self.assertIn("missing_censipam_context", score["gates"])

    def test_reference_regression_changes_only_shadow_and_keeps_gold_hash(self) -> None:
        output_dir = Path.cwd() / "tests" / "_tmp_act_shadow"
        if output_dir.exists():
            shutil.rmtree(output_dir, ignore_errors=True)
        (output_dir / "candidates").mkdir(parents=True)
        try:
            processo = "60093.000015/2020-60"
            self._write_candidate(
                output_dir, processo, "tree_rank_001_id_documento_2208807_act_final",
                "Acordo de Cooperação Técnica 04/2020 (2208807)",
                self._act_text("04/2020", processo, body="Portaria de nomeação. O plano de trabalho integra o acordo."),
                "https://sei.test/?id_documento=2208807", 1,
            )
            self._write_candidate(
                output_dir, processo, "tree_rank_003_id_anexo_1139528_act_final",
                "Acordo de Cooperação Técnica 7/2020/SEMA (2258213)",
                self._act_text("7/2020/SEMA", "0820.009800.00127/2020-86"),
                "https://sei.test/?id_anexo=1139528", 3,
            )
            self._write_candidate(
                output_dir, processo, "tree_rank_009_id_documento_9395250_act_final",
                "Relatório 48 - Encerramento de ACT - SEMA/AC (8298656)",
                "RELATÓRIO DE ENCERRAMENTO relativo ao Acordo de Cooperação Técnica 04/2020. " + self._act_text("04/2020", processo),
                "https://sei.test/?id_documento=9395250", 9,
            )
            self._write_csv(
                output_dir / "parcerias_vigentes_latest.csv",
                [{"interno_descricao": "PARCERIAS VIGENTES", "seq": "12", "processo": processo, "parceiro": "SEMA - ACRE", "vigencia": "60 meses", "numero_act": "04/2020", "objeto": "Cooperação"}],
            )
            self._write_csv(output_dir / "act_status_execucao_latest.csv", [{"processo": processo, "found": "True"}])
            export_normalized_csv(output_dir)
            gold = output_dir / "acordo_cooperacao_tecnica_60093.000015_2020-60.json"
            gold_before = hashlib.sha256(gold.read_bytes()).hexdigest()
            result = export_shadow_report(output_dir)
            gold_after = hashlib.sha256(gold.read_bytes()).hexdigest()
            detail = json.loads((output_dir / "act_shadow_comparison_latest.json").read_text(encoding="utf-8"))["processes"][0]

            self.assertEqual(detail["current_selected_candidate"], "1139528")
            self.assertEqual(detail["shadow_selected_candidate"], "2208807")
            self.assertTrue(detail["winner_changed"])
            report = next(item for item in detail["candidates"] if item["candidate_id"] == "9395250")
            self.assertFalse(report["shadow_score_breakdown"]["eligible"])
            self.assertEqual(gold_before, gold_after)
            self.assertTrue(result["metrics"]["gold_immutable"])
            self.assertEqual(detail["shadow_scoring_version"], SHADOW_SCORING_VERSION)
        finally:
            shutil.rmtree(output_dir, ignore_errors=True)

    @staticmethod
    def _act_text(number: str, processo: str, body: str = "") -> str:
        return f"""
            ACORDO DE COOPERAÇÃO TÉCNICA Nº {number}
            PROCESSO Nº {processo}
            ACORDO DE COOPERAÇÃO TÉCNICA QUE ENTRE SI CELEBRAM A UNIÃO,
            REPRESENTADA PELO MINISTÉRIO DA DEFESA, POR INTERMÉDIO DO CENSIPAM,
            E A SECRETARIA DE ESTADO DO MEIO AMBIENTE - SEMA.
            CLÁUSULA PRIMEIRA - DO OBJETO
            O objeto do presente Acordo é a cooperação técnica institucional.
            {body}
            CLÁUSULA NONA - DA VIGÊNCIA
            A vigência será de 60 meses a partir da assinatura.
            Documento assinado eletronicamente por Primeiro Signatário, em 01/05/2020.
            Documento assinado eletronicamente por Segundo Signatário, em 02/05/2020.
        """

    @staticmethod
    def _record(*, title: str, text: str, numero: str, alignment: str) -> dict:
        return {
            "processo": "60093.000015/2020-60", "doc_class": "act_final", "validation_status": "valid_for_requested_type",
            "process_alignment_status": alignment, "document_processo": "60093.000015/2020-60" if alignment == "aligned" else "",
            "numero_acordo": numero, "objeto": "Cooperação", "vigencia_raw": "60 meses", "datas_assinatura": "2020-05-01 | 2020-05-02",
            "orgao_convenente": "SEMA", "has_internal_context": True, "relatorio_encerramento": False,
            "party_extraction": {"internal_party": {"role": "parte_interna"}},
            "payload": {"snapshot": {"title": title, "text": text}, "collection": {"chosen_documento": title}},
        }

    @staticmethod
    def _write_candidate(output_dir: Path, processo: str, suffix: str, title: str, text: str, url: str, rank: int) -> None:
        payload = {
            "processo": processo, "documento": processo, "requested_type": "act",
            "snapshot": {"title": title, "text": text, "url": url, "extraction_mode": "html_dom", "tables": []},
            "collection": {"found": True, "found_in": "tree", "chosen_documento": title, "selection_detail": f"rank={rank}/9 score=131 termos=acordo de cooperacao tecnica"},
        }
        safe = processo.replace("/", "_")
        path = output_dir / "candidates" / f"acordo_cooperacao_tecnica_{safe}_{suffix}.json"
        path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    @staticmethod
    def _write_csv(path: Path, rows: list[dict]) -> None:
        with path.open("w", encoding="utf-8-sig", newline="") as file_obj:
            writer = csv.DictWriter(file_obj, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)


if __name__ == "__main__":
    unittest.main()
