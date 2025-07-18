"""
Regulatory Reporting API endpoints for Compliance Service.
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, date
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
import asyncio

from platformq_shared import get_current_user
from ..services.regulatory_reporter import RegulatoryReporter

logger = logging.getLogger(__name__)

router = APIRouter()


class ReportRequest(BaseModel):
    """Request to generate a regulatory report"""
    report_type: str = Field(..., description="Type of report (SAR, CTR, etc.)")
    jurisdiction: str = Field(..., description="Regulatory jurisdiction")
    start_date: date = Field(..., description="Report period start")
    end_date: date = Field(..., description="Report period end")
    include_details: bool = Field(True, description="Include detailed transactions")


class SARSubmission(BaseModel):
    """Suspicious Activity Report submission"""
    user_id: str = Field(..., description="Subject of the SAR")
    activity_type: str = Field(..., description="Type of suspicious activity")
    description: str = Field(..., description="Detailed description")
    transaction_ids: List[str] = Field(default_factory=list)
    amount_involved: Optional[float] = None
    jurisdiction: str = Field(..., description="Filing jurisdiction")


def get_regulatory_reporter(request) -> RegulatoryReporter:
    """Dependency to get regulatory reporter instance"""
    return request.app.state.regulatory_reporter


@router.post("/generate")
async def generate_report(
    request: ReportRequest,
    current_user: Dict = Depends(get_current_user),
    reporter: RegulatoryReporter = Depends(get_regulatory_reporter)
):
    """
    Generate a regulatory report.
    
    Creates reports for various regulatory requirements (SAR, CTR, etc.).
    """
    try:
        if "compliance_officer" not in current_user.get("roles", []):
            raise HTTPException(
                status_code=403,
                detail="Only compliance officers can generate reports"
            )
        
        # Generate report
        report = await reporter.generate_report(
            report_type=request.report_type,
            jurisdiction=request.jurisdiction,
            start_date=request.start_date,
            end_date=request.end_date,
            include_details=request.include_details,
            generated_by=current_user["id"]
        )
        
        return {
            "report_id": report["id"],
            "report_type": request.report_type,
            "jurisdiction": request.jurisdiction,
            "status": "generated",
            "download_url": f"/api/v1/reporting/download/{report['id']}",
            "expires_at": report["expires_at"]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating report: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/sar/submit")
async def submit_sar(
    submission: SARSubmission,
    current_user: Dict = Depends(get_current_user),
    reporter: RegulatoryReporter = Depends(get_regulatory_reporter)
):
    """
    Submit a Suspicious Activity Report.
    
    Files SAR with appropriate regulatory authority.
    """
    try:
        if "compliance_officer" not in current_user.get("roles", []):
            raise HTTPException(
                status_code=403,
                detail="Only compliance officers can file SARs"
            )
        
        # Create and submit SAR
        sar = await reporter.submit_sar(
            user_id=submission.user_id,
            activity_type=submission.activity_type,
            description=submission.description,
            transaction_ids=submission.transaction_ids,
            amount_involved=submission.amount_involved,
            jurisdiction=submission.jurisdiction,
            filed_by=current_user["id"]
        )
        
        return {
            "sar_id": sar["id"],
            "filing_number": sar["filing_number"],
            "status": "submitted",
            "jurisdiction": submission.jurisdiction,
            "filed_at": sar["filed_at"]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error submitting SAR: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/history")
async def get_reporting_history(
    report_type: Optional[str] = Query(None),
    jurisdiction: Optional[str] = Query(None),
    days: int = Query(90),
    current_user: Dict = Depends(get_current_user),
    reporter: RegulatoryReporter = Depends(get_regulatory_reporter)
):
    """
    Get regulatory reporting history.
    
    Shows previously filed reports and their status.
    """
    try:
        if "compliance_officer" not in current_user.get("roles", []):
            raise HTTPException(
                status_code=403,
                detail="Only compliance officers can view reporting history"
            )
        
        history = await reporter.get_reporting_history(
            report_type=report_type,
            jurisdiction=jurisdiction,
            days=days
        )
        
        return {
            "reports": [
                {
                    "report_id": r["id"],
                    "report_type": r["type"],
                    "jurisdiction": r["jurisdiction"],
                    "filed_at": r["filed_at"],
                    "status": r["status"],
                    "filing_number": r.get("filing_number")
                }
                for r in history
            ],
            "total": len(history)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting reporting history: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/deadlines")
async def get_reporting_deadlines(
    current_user: Dict = Depends(get_current_user),
    reporter: RegulatoryReporter = Depends(get_regulatory_reporter)
):
    """
    Get upcoming regulatory reporting deadlines.
    
    Shows required reports and filing deadlines by jurisdiction.
    """
    try:
        if "compliance_officer" not in current_user.get("roles", []):
            raise HTTPException(
                status_code=403,
                detail="Only compliance officers can view deadlines"
            )
        
        deadlines = await reporter.get_upcoming_deadlines()
        
        return {
            "deadlines": [
                {
                    "report_type": d["type"],
                    "jurisdiction": d["jurisdiction"],
                    "due_date": d["due_date"],
                    "frequency": d["frequency"],
                    "last_filed": d.get("last_filed"),
                    "status": d["status"]
                }
                for d in deadlines
            ]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting deadlines: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/jurisdictions")
async def get_supported_jurisdictions(
    current_user: Dict = Depends(get_current_user),
    reporter: RegulatoryReporter = Depends(get_regulatory_reporter)
):
    """
    Get list of supported regulatory jurisdictions.
    
    Shows available jurisdictions and their reporting requirements.
    """
    try:
        jurisdictions = await reporter.get_supported_jurisdictions()
        
        return {
            "jurisdictions": [
                {
                    "code": j["code"],
                    "name": j["name"],
                    "report_types": j["report_types"],
                    "filing_methods": j["filing_methods"],
                    "requirements": j["requirements"]
                }
                for j in jurisdictions
            ]
        }
        
    except Exception as e:
        logger.error(f"Error getting jurisdictions: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/download/{report_id}")
async def download_report(
    report_id: str,
    current_user: Dict = Depends(get_current_user),
    reporter: RegulatoryReporter = Depends(get_regulatory_reporter)
):
    """
    Download a generated regulatory report.
    
    Returns the report file for download.
    """
    try:
        if "compliance_officer" not in current_user.get("roles", []):
            raise HTTPException(
                status_code=403,
                detail="Only compliance officers can download reports"
            )
        
        # Get report file
        report_data = await reporter.get_report_file(report_id)
        
        return {
            "report_id": report_id,
            "filename": report_data["filename"],
            "content_type": report_data["content_type"],
            "data": report_data["data"]  # Base64 encoded
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error downloading report: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/schedule")
async def schedule_recurring_report(
    report_type: str,
    jurisdiction: str,
    frequency: str,  # daily, weekly, monthly, quarterly
    parameters: Dict[str, Any],
    current_user: Dict = Depends(get_current_user),
    reporter: RegulatoryReporter = Depends(get_regulatory_reporter)
):
    """
    Schedule a recurring regulatory report.
    
    Automates generation of periodic reports.
    """
    try:
        if "compliance_officer" not in current_user.get("roles", []):
            raise HTTPException(
                status_code=403,
                detail="Only compliance officers can schedule reports"
            )
        
        schedule = await reporter.schedule_report(
            report_type=report_type,
            jurisdiction=jurisdiction,
            frequency=frequency,
            parameters=parameters,
            scheduled_by=current_user["id"]
        )
        
        return {
            "schedule_id": schedule["id"],
            "report_type": report_type,
            "frequency": frequency,
            "next_run": schedule["next_run"],
            "status": "scheduled"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error scheduling report: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/statistics")
async def get_reporting_statistics(
    year: int = Query(datetime.now().year),
    current_user: Dict = Depends(get_current_user),
    reporter: RegulatoryReporter = Depends(get_regulatory_reporter)
):
    """
    Get regulatory reporting statistics.
    
    Shows report counts, trends, and compliance metrics.
    """
    try:
        if "compliance_officer" not in current_user.get("roles", []):
            raise HTTPException(
                status_code=403,
                detail="Only compliance officers can view statistics"
            )
        
        stats = await reporter.get_statistics(year=year)
        
        return {
            "year": year,
            "total_reports": stats["total_reports"],
            "reports_by_type": stats["reports_by_type"],
            "reports_by_jurisdiction": stats["reports_by_jurisdiction"],
            "sars_filed": stats["sars_filed"],
            "compliance_rate": stats["compliance_rate"],
            "average_filing_time": stats["average_filing_time"]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting statistics: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 