"""
LLM-Powered Testing Report Generator

Generates comprehensive validation reports using local LLM (Ollama)
"""

import json
import requests
from typing import Dict, List, Optional, Any
from datetime import datetime
import pandas as pd
import numpy as np


class NumpyEncoder(json.JSONEncoder):
    """Custom JSON encoder for numpy types"""
    def default(self, obj):
        if isinstance(obj, (np.integer, np.floating)):
            return obj.item()
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, (np.bool_, bool)):
            return bool(obj)
        return super().default(obj)


class LLMReportGenerator:
    """Generate testing reports using local LLM"""

    def __init__(
        self,
        ollama_url: str = "http://localhost:11434",
        model: str = "llama3.2:3b"
    ):
        """
        Initialize LLM report generator

        Args:
            ollama_url: Ollama API endpoint
            model: Model to use (llama3.2:3b, mistral, etc.)
        """
        self.ollama_url = ollama_url
        self.model = model
        self.report_data = {
            'timestamp': datetime.now().isoformat(),
            'validations': [],
            'metrics': {},
            'previous_results': None
        }

    def check_ollama_status(self) -> bool:
        """Check if Ollama is running"""
        try:
            response = requests.get(f"{self.ollama_url}/api/tags", timeout=5)
            return response.status_code == 200
        except:
            return False

    def list_available_models(self) -> List[str]:
        """List available Ollama models"""
        try:
            response = requests.get(f"{self.ollama_url}/api/tags", timeout=5)
            if response.status_code == 200:
                models = response.json().get('models', [])
                return [m['name'] for m in models]
            return []
        except:
            return []

    def query_llm(self, prompt: str, system_prompt: Optional[str] = None) -> str:
        """
        Query local LLM via Ollama

        Args:
            prompt: User prompt
            system_prompt: Optional system prompt

        Returns:
            LLM response text
        """
        payload = {
            "model": self.model,
            "prompt": prompt,
            "stream": False
        }

        if system_prompt:
            payload["system"] = system_prompt

        try:
            response = requests.post(
                f"{self.ollama_url}/api/generate",
                json=payload,
                timeout=120
            )

            if response.status_code == 200:
                return response.json()['response']
            else:
                return f"Error: {response.status_code} - {response.text}"

        except Exception as e:
            return f"Error querying LLM: {str(e)}"

    def add_validation_result(
        self,
        metric_name: str,
        passed: bool,
        details: Dict[str, Any]
    ):
        """Add validation result to report data"""
        self.report_data['validations'].append({
            'metric': metric_name,
            'passed': passed,
            'details': details,
            'timestamp': datetime.now().isoformat()
        })

    def add_metrics(self, metrics: Dict[str, Any]):
        """Add data quality metrics"""
        self.report_data['metrics'].update(metrics)

    def load_previous_results(self, previous_report_path: str):
        """Load previous validation results for comparison"""
        try:
            with open(previous_report_path, 'r') as f:
                self.report_data['previous_results'] = json.load(f)
        except:
            self.report_data['previous_results'] = None

    def generate_executive_summary(self) -> str:
        """Generate executive summary using LLM"""

        validation_summary = {
            'total': len(self.report_data['validations']),
            'passed': sum(1 for v in self.report_data['validations'] if v['passed']),
            'failed': sum(1 for v in self.report_data['validations'] if not v['passed']),
            'pass_rate': (sum(1 for v in self.report_data['validations'] if v['passed']) /
                         len(self.report_data['validations']) * 100) if self.report_data['validations'] else 0
        }

        prompt = f"""
Generate a concise executive summary for a CDL (Common Data Layer) validation report.

Validation Results:
- Total validations: {validation_summary['total']}
- Passed: {validation_summary['passed']}
- Failed: {validation_summary['failed']}
- Pass rate: {validation_summary['pass_rate']:.1f}%

Data Metrics:
{json.dumps(self.report_data['metrics'], indent=2)}

Focus on:
1. Overall health status (Green/Yellow/Red)
2. Key findings (2-3 bullet points)
3. Business impact assessment
4. Recommended actions (if any issues found)

Keep it executive-level (non-technical), max 150 words.
"""

        system_prompt = """You are a data quality analyst writing executive summaries for senior stakeholders.
Be concise, clear, and focus on business impact. Use professional language."""

        return self.query_llm(prompt, system_prompt)

    def generate_detailed_findings(self) -> str:
        """Generate detailed findings with explanations"""

        failed_validations = [v for v in self.report_data['validations'] if not v['passed']]

        if not failed_validations:
            return "✅ All validations passed. No issues found."

        prompt = f"""
Analyze these failed validation tests and provide detailed explanations:

Failed Validations:
{json.dumps(failed_validations, indent=2)}

For each failure, provide:
1. What failed and why
2. Potential root causes (technical explanation)
3. Data quality implications
4. Impact on downstream systems

Format as a structured analysis with clear sections.
"""

        system_prompt = """You are a data engineer analyzing validation failures.
Provide technical depth while remaining clear. Focus on actionable insights."""

        return self.query_llm(prompt, system_prompt)

    def generate_recommendations(self) -> str:
        """Generate recommendations for fixes"""

        failed_validations = [v for v in self.report_data['validations'] if not v['passed']]

        if not failed_validations:
            return "✅ No issues detected. Continue monitoring data quality trends."

        prompt = f"""
Based on these validation failures, provide specific recommendations:

Failures:
{json.dumps(failed_validations, indent=2)}

Data Context:
{json.dumps(self.report_data['metrics'], indent=2)}

Provide:
1. Immediate actions (priority order)
2. Medium-term fixes
3. Long-term improvements
4. Monitoring enhancements
5. Risk mitigation strategies

Be specific and actionable.
"""

        system_prompt = """You are a data quality expert providing remediation guidance.
Focus on practical, implementable solutions with clear priorities."""

        return self.query_llm(prompt, system_prompt)

    def generate_comparison_analysis(self) -> str:
        """Generate comparison with previous runs"""

        if not self.report_data['previous_results']:
            return "ℹ️ No previous results available for comparison."

        current_pass_rate = (sum(1 for v in self.report_data['validations'] if v['passed']) /
                            len(self.report_data['validations']) * 100) if self.report_data['validations'] else 0

        prev_validations = self.report_data['previous_results'].get('validations', [])
        prev_pass_rate = (sum(1 for v in prev_validations if v['passed']) /
                         len(prev_validations) * 100) if prev_validations else 0

        prompt = f"""
Compare current validation results with previous run:

Current Run:
- Pass rate: {current_pass_rate:.1f}%
- Failed tests: {[v['metric'] for v in self.report_data['validations'] if not v['passed']]}

Previous Run:
- Pass rate: {prev_pass_rate:.1f}%
- Failed tests: {[v['metric'] for v in prev_validations if not v['passed']]}

Provide:
1. Trend analysis (improving/degrading/stable)
2. New issues introduced
3. Previously failing tests now passing
4. Persistent issues
5. Overall trajectory assessment

Be analytical and highlight significant changes.
"""

        system_prompt = """You are a data quality analyst tracking trends over time.
Focus on changes, patterns, and trajectory."""

        return self.query_llm(prompt, system_prompt)

    def generate_risk_assessment(self) -> str:
        """Generate risk assessment"""

        failed_count = sum(1 for v in self.report_data['validations'] if not v['passed'])
        total_count = len(self.report_data['validations'])

        prompt = f"""
Assess risks based on validation results:

Results Summary:
- Total validations: {total_count}
- Failed: {failed_count}
- Critical metrics affected: {json.dumps([v['metric'] for v in self.report_data['validations'] if not v['passed']], indent=2)}

Data Context:
{json.dumps(self.report_data['metrics'], indent=2)}

Provide risk assessment:
1. Overall risk level (Low/Medium/High/Critical)
2. Specific risk factors identified
3. Downstream impact analysis
4. Data trust implications
5. Mitigation urgency

Consider business criticality of affected metrics.
"""

        system_prompt = """You are a data governance expert assessing data quality risks.
Be thorough but balanced. Focus on business impact."""

        return self.query_llm(prompt, system_prompt)

    def generate_comprehensive_report(
        self,
        output_format: str = "markdown"
    ) -> str:
        """
        Generate comprehensive testing report

        Args:
            output_format: 'markdown' or 'html'

        Returns:
            Formatted report string
        """

        # Check Ollama availability
        if not self.check_ollama_status():
            return self._generate_fallback_report()

        print("Generating comprehensive testing report using LLM...")

        # Generate all sections
        print("  Generating executive summary...")
        exec_summary = self.generate_executive_summary()

        print("  Analyzing detailed findings...")
        detailed_findings = self.generate_detailed_findings()

        print("  Creating recommendations...")
        recommendations = self.generate_recommendations()

        print("  Comparing with previous runs...")
        comparison = self.generate_comparison_analysis()

        print("  Assessing risks...")
        risk_assessment = self.generate_risk_assessment()

        # Compile report
        if output_format == "markdown":
            report = self._format_markdown_report(
                exec_summary,
                detailed_findings,
                recommendations,
                comparison,
                risk_assessment
            )
        else:
            report = self._format_html_report(
                exec_summary,
                detailed_findings,
                recommendations,
                comparison,
                risk_assessment
            )

        return report

    def _format_markdown_report(
        self,
        exec_summary: str,
        findings: str,
        recommendations: str,
        comparison: str,
        risk: str
    ) -> str:
        """Format report as Markdown"""

        validation_stats = {
            'total': len(self.report_data['validations']),
            'passed': sum(1 for v in self.report_data['validations'] if v['passed']),
            'failed': sum(1 for v in self.report_data['validations'] if not v['passed'])
        }

        report = f"""
# CDL Validation Testing Report

**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
**LLM Model:** {self.model}

---

## Executive Summary

{exec_summary}

---

## Validation Statistics

| Metric | Value |
|--------|-------|
| Total Validations | {validation_stats['total']} |
| Passed | ✅ {validation_stats['passed']} |
| Failed | ❌ {validation_stats['failed']} |
| Pass Rate | {(validation_stats['passed'] / validation_stats['total'] * 100) if validation_stats['total'] > 0 else 0:.1f}% |

---

## Data Quality Metrics

```json
{json.dumps(self.report_data['metrics'], indent=2)}
```

---

## Detailed Findings

{findings}

---

## Recommendations

{recommendations}

---

## Comparison with Previous Run

{comparison}

---

## Risk Assessment

{risk}

---

## Validation Details

"""

        # Add individual validation results
        for v in self.report_data['validations']:
            status = "✅ PASS" if v['passed'] else "❌ FAIL"
            report += f"\n### {v['metric']} - {status}\n\n"

            # Convert details to JSON-serializable format
            details_serializable = {}
            for key, value in v['details'].items():
                if isinstance(value, (bool, int, float, str, type(None))):
                    details_serializable[key] = value
                else:
                    details_serializable[key] = str(value)

            report += f"```json\n{json.dumps(details_serializable, indent=2)}\n```\n"

        report += "\n---\n\n*Report generated by LLM-powered CDL Validation Framework*\n"

        return report

    def _format_html_report(
        self,
        exec_summary: str,
        findings: str,
        recommendations: str,
        comparison: str,
        risk: str
    ) -> str:
        """Format report as HTML"""
        # HTML implementation
        return f"<html><body><h1>CDL Validation Report</h1><p>{exec_summary}</p></body></html>"

    def _generate_fallback_report(self) -> str:
        """Generate basic report without LLM if Ollama is unavailable"""

        validation_stats = {
            'total': len(self.report_data['validations']),
            'passed': sum(1 for v in self.report_data['validations'] if v['passed']),
            'failed': sum(1 for v in self.report_data['validations'] if not v['passed'])
        }

        report = f"""
# CDL Validation Testing Report

**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
**Note:** LLM unavailable - using fallback report format

## Summary

- Total Validations: {validation_stats['total']}
- Passed: ✅ {validation_stats['passed']}
- Failed: ❌ {validation_stats['failed']}
- Pass Rate: {(validation_stats['passed'] / validation_stats['total'] * 100) if validation_stats['total'] > 0 else 0:.1f}%

## Validation Results

"""

        for v in self.report_data['validations']:
            status = "✅ PASS" if v['passed'] else "❌ FAIL"
            report += f"\n### {v['metric']} - {status}\n\n"
            report += f"```json\n{json.dumps(v['details'], indent=2)}\n```\n"

        return report

    def save_report(self, filepath: str, content: str):
        """Save report to file"""
        with open(filepath, 'w') as f:
            f.write(content)

        # Also save raw data as JSON for future comparisons
        json_path = filepath.replace('.md', '.json').replace('.html', '.json')
        with open(json_path, 'w') as f:
            json.dump(self.report_data, f, indent=2, cls=NumpyEncoder)
