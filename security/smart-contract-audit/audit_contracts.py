#!/usr/bin/env python3
"""
Smart Contract Security Audit Script
Performs comprehensive security analysis on marketplace smart contracts
"""

import os
import sys
import json
import subprocess
import re
from typing import Dict, List, Any, Tuple
from dataclasses import dataclass
from datetime import datetime
import argparse

@dataclass
class SecurityIssue:
    severity: str  # critical, high, medium, low, info
    category: str
    contract: str
    function: str
    line: int
    description: str
    recommendation: str

class SmartContractAuditor:
    """Comprehensive smart contract security auditor"""
    
    def __init__(self, contracts_dir: str):
        self.contracts_dir = contracts_dir
        self.issues: List[SecurityIssue] = []
        self.stats = {
            "contracts_analyzed": 0,
            "total_issues": 0,
            "critical": 0,
            "high": 0,
            "medium": 0,
            "low": 0,
            "info": 0
        }
    
    def audit_all_contracts(self) -> Dict[str, Any]:
        """Run comprehensive audit on all contracts"""
        print("🔍 Starting Smart Contract Security Audit\n")
        
        # Find all Solidity files
        contracts = self._find_contracts()
        self.stats["contracts_analyzed"] = len(contracts)
        
        for contract_path in contracts:
            print(f"📋 Auditing: {os.path.basename(contract_path)}")
            self._audit_contract(contract_path)
        
        # Generate report
        report = self._generate_report()
        
        print(f"\n✅ Audit Complete: {self.stats['total_issues']} issues found")
        return report
    
    def _find_contracts(self) -> List[str]:
        """Find all Solidity contracts"""
        contracts = []
        for root, dirs, files in os.walk(self.contracts_dir):
            for file in files:
                if file.endswith('.sol'):
                    contracts.append(os.path.join(root, file))
        return contracts
    
    def _audit_contract(self, contract_path: str):
        """Audit a single contract"""
        contract_name = os.path.basename(contract_path).replace('.sol', '')
        
        # 1. Run Slither analysis
        self._run_slither(contract_path, contract_name)
        
        # 2. Check for common vulnerabilities
        self._check_reentrancy(contract_path, contract_name)
        self._check_integer_overflow(contract_path, contract_name)
        self._check_access_control(contract_path, contract_name)
        self._check_timestamp_dependence(contract_path, contract_name)
        self._check_gas_limits(contract_path, contract_name)
        
        # 3. Check marketplace-specific issues
        self._check_royalty_logic(contract_path, contract_name)
        self._check_escrow_logic(contract_path, contract_name)
        self._check_nft_minting(contract_path, contract_name)
        
        # 4. Check DeFi-specific issues
        self._check_flash_loan_attacks(contract_path, contract_name)
        self._check_oracle_manipulation(contract_path, contract_name)
        self._check_liquidity_issues(contract_path, contract_name)
    
    def _run_slither(self, contract_path: str, contract_name: str):
        """Run Slither static analysis"""
        try:
            result = subprocess.run(
                ['slither', contract_path, '--json', '-'],
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0:
                findings = json.loads(result.stdout)
                for detector in findings.get('detectors', []):
                    severity = detector['impact']
                    if severity == 'High':
                        severity = 'high'
                    elif severity == 'Medium':
                        severity = 'medium'
                    elif severity == 'Low':
                        severity = 'low'
                    else:
                        severity = 'info'
                    
                    for element in detector.get('elements', []):
                        self._add_issue(
                            severity=severity,
                            category='slither',
                            contract=contract_name,
                            function=element.get('name', 'N/A'),
                            line=element.get('source_mapping', {}).get('lines', [0])[0],
                            description=detector['description'],
                            recommendation=detector.get('recommendation', 'Review and fix the issue')
                        )
        except Exception as e:
            print(f"  ⚠️  Slither analysis failed: {e}")
    
    def _check_reentrancy(self, contract_path: str, contract_name: str):
        """Check for reentrancy vulnerabilities"""
        with open(contract_path, 'r') as f:
            content = f.read()
            lines = content.split('\n')
        
        # Pattern: external calls before state changes
        for i, line in enumerate(lines):
            if '.call(' in line or '.transfer(' in line or '.send(' in line:
                # Check if state changes occur after the external call
                for j in range(i + 1, min(i + 10, len(lines))):
                    if '=' in lines[j] and ('mapping' in lines[j] or 'storage' in lines[j]):
                        self._add_issue(
                            severity='critical',
                            category='reentrancy',
                            contract=contract_name,
                            function=self._get_function_name(lines, i),
                            line=i + 1,
                            description='Potential reentrancy vulnerability: state change after external call',
                            recommendation='Use checks-effects-interactions pattern or ReentrancyGuard'
                        )
    
    def _check_integer_overflow(self, contract_path: str, contract_name: str):
        """Check for integer overflow/underflow"""
        with open(contract_path, 'r') as f:
            content = f.read()
            lines = content.split('\n')
        
        # Check for arithmetic operations without SafeMath
        if 'using SafeMath' not in content and 'pragma solidity ^0.8' not in content:
            for i, line in enumerate(lines):
                if any(op in line for op in ['+', '-', '*', '/']) and 'uint' in line:
                    self._add_issue(
                        severity='high',
                        category='integer-overflow',
                        contract=contract_name,
                        function=self._get_function_name(lines, i),
                        line=i + 1,
                        description='Potential integer overflow/underflow without SafeMath',
                        recommendation='Use SafeMath library or Solidity 0.8+ with built-in overflow checks'
                    )
    
    def _check_access_control(self, contract_path: str, contract_name: str):
        """Check for access control issues"""
        with open(contract_path, 'r') as f:
            content = f.read()
            lines = content.split('\n')
        
        # Check for missing access modifiers on sensitive functions
        sensitive_patterns = ['mint', 'burn', 'pause', 'unpause', 'transfer', 'approve']
        for i, line in enumerate(lines):
            if 'function' in line and any(pattern in line.lower() for pattern in sensitive_patterns):
                if not any(modifier in line for modifier in ['onlyOwner', 'onlyRole', 'private', 'internal']):
                    self._add_issue(
                        severity='high',
                        category='access-control',
                        contract=contract_name,
                        function=self._get_function_name(lines, i),
                        line=i + 1,
                        description='Sensitive function without access control modifier',
                        recommendation='Add appropriate access control modifiers (onlyOwner, onlyRole, etc.)'
                    )
    
    def _check_timestamp_dependence(self, contract_path: str, contract_name: str):
        """Check for timestamp dependence"""
        with open(contract_path, 'r') as f:
            content = f.read()
            lines = content.split('\n')
        
        timestamp_keywords = ['block.timestamp', 'now']
        for i, line in enumerate(lines):
            if any(keyword in line for keyword in timestamp_keywords):
                # Check if used in critical logic
                if any(op in line for op in ['<', '>', '<=', '>=', '==']):
                    self._add_issue(
                        severity='medium',
                        category='timestamp-dependence',
                        contract=contract_name,
                        function=self._get_function_name(lines, i),
                        line=i + 1,
                        description='Timestamp used in critical logic - can be manipulated by miners',
                        recommendation='Use block numbers instead of timestamps for critical logic'
                    )
    
    def _check_gas_limits(self, contract_path: str, contract_name: str):
        """Check for gas limit issues"""
        with open(contract_path, 'r') as f:
            content = f.read()
            lines = content.split('\n')
        
        # Check for unbounded loops
        for i, line in enumerate(lines):
            if 'for' in line or 'while' in line:
                # Look for array iterations without bounds
                if 'length' in line and not any(bound in line for bound in ['<', 'limit', 'max']):
                    self._add_issue(
                        severity='medium',
                        category='gas-limit',
                        contract=contract_name,
                        function=self._get_function_name(lines, i),
                        line=i + 1,
                        description='Unbounded loop can cause out-of-gas errors',
                        recommendation='Add upper bounds to loops or use pagination'
                    )
    
    def _check_royalty_logic(self, contract_path: str, contract_name: str):
        """Check marketplace-specific royalty logic"""
        with open(contract_path, 'r') as f:
            content = f.read()
            lines = content.split('\n')
        
        # Check royalty calculations
        for i, line in enumerate(lines):
            if 'royalty' in line.lower() and ('*' in line or '/' in line):
                # Check for precision loss
                if '/' in line and not '10000' in line:
                    self._add_issue(
                        severity='medium',
                        category='royalty-calculation',
                        contract=contract_name,
                        function=self._get_function_name(lines, i),
                        line=i + 1,
                        description='Potential precision loss in royalty calculation',
                        recommendation='Use basis points (10000) for percentage calculations'
                    )
    
    def _check_escrow_logic(self, contract_path: str, contract_name: str):
        """Check escrow implementation"""
        if 'escrow' in contract_name.lower():
            with open(contract_path, 'r') as f:
                content = f.read()
            
            # Check for proper withdrawal patterns
            if 'withdraw' in content and 'balance[' not in content:
                self._add_issue(
                    severity='high',
                    category='escrow-pattern',
                    contract=contract_name,
                    function='withdraw',
                    line=0,
                    description='Escrow without proper balance tracking',
                    recommendation='Track individual balances in mapping'
                )
    
    def _check_nft_minting(self, contract_path: str, contract_name: str):
        """Check NFT minting logic"""
        if 'mint' in contract_name.lower() or 'nft' in contract_name.lower():
            with open(contract_path, 'r') as f:
                content = f.read()
                lines = content.split('\n')
            
            # Check for proper minting limits
            for i, line in enumerate(lines):
                if 'mint' in line and 'function' in line:
                    # Check if there's a supply limit check
                    function_end = i + 20  # Check next 20 lines
                    has_limit_check = False
                    for j in range(i, min(function_end, len(lines))):
                        if 'maxSupply' in lines[j] or 'totalSupply' in lines[j]:
                            has_limit_check = True
                            break
                    
                    if not has_limit_check:
                        self._add_issue(
                            severity='medium',
                            category='nft-minting',
                            contract=contract_name,
                            function='mint',
                            line=i + 1,
                            description='NFT minting without supply limit check',
                            recommendation='Add maximum supply check to prevent unlimited minting'
                        )
    
    def _check_flash_loan_attacks(self, contract_path: str, contract_name: str):
        """Check for flash loan attack vulnerabilities"""
        if 'lending' in contract_name.lower() or 'defi' in contract_name.lower():
            with open(contract_path, 'r') as f:
                content = f.read()
            
            # Check for price manipulation vulnerabilities
            if 'getPrice' in content or 'oracle' in content:
                if 'TWAP' not in content and 'timeWeighted' not in content:
                    self._add_issue(
                        severity='critical',
                        category='flash-loan',
                        contract=contract_name,
                        function='price-oracle',
                        line=0,
                        description='Price oracle vulnerable to flash loan manipulation',
                        recommendation='Use time-weighted average price (TWAP) oracles'
                    )
    
    def _check_oracle_manipulation(self, contract_path: str, contract_name: str):
        """Check for oracle manipulation risks"""
        with open(contract_path, 'r') as f:
            content = f.read()
        
        # Check for single oracle dependency
        if 'oracle' in content.lower():
            oracle_count = content.lower().count('oracle')
            if oracle_count == 1:
                self._add_issue(
                    severity='high',
                    category='oracle-risk',
                    contract=contract_name,
                    function='oracle-dependency',
                    line=0,
                    description='Single oracle dependency - single point of failure',
                    recommendation='Use multiple oracle sources or decentralized oracles like Chainlink'
                )
    
    def _check_liquidity_issues(self, contract_path: str, contract_name: str):
        """Check for liquidity-related issues"""
        if 'pool' in contract_name.lower() or 'liquidity' in contract_name.lower():
            with open(contract_path, 'r') as f:
                content = f.read()
                lines = content.split('\n')
            
            # Check for sandwich attack protection
            has_slippage_protection = 'minAmount' in content or 'slippage' in content
            if not has_slippage_protection:
                self._add_issue(
                    severity='high',
                    category='liquidity',
                    contract=contract_name,
                    function='swap/trade',
                    line=0,
                    description='No slippage protection - vulnerable to sandwich attacks',
                    recommendation='Add minimum output amount checks for swaps'
                )
    
    def _get_function_name(self, lines: List[str], line_num: int) -> str:
        """Extract function name from context"""
        # Search backwards for function declaration
        for i in range(line_num, max(0, line_num - 20), -1):
            if 'function' in lines[i]:
                match = re.search(r'function\s+(\w+)', lines[i])
                if match:
                    return match.group(1)
        return 'unknown'
    
    def _add_issue(self, severity: str, category: str, contract: str, 
                   function: str, line: int, description: str, recommendation: str):
        """Add a security issue"""
        issue = SecurityIssue(
            severity=severity,
            category=category,
            contract=contract,
            function=function,
            line=line,
            description=description,
            recommendation=recommendation
        )
        self.issues.append(issue)
        self.stats["total_issues"] += 1
        self.stats[severity] += 1
    
    def _generate_report(self) -> Dict[str, Any]:
        """Generate audit report"""
        report = {
            "audit_date": datetime.now().isoformat(),
            "summary": self.stats,
            "issues": []
        }
        
        # Sort issues by severity
        severity_order = {'critical': 0, 'high': 1, 'medium': 2, 'low': 3, 'info': 4}
        sorted_issues = sorted(self.issues, key=lambda x: severity_order.get(x.severity, 5))
        
        for issue in sorted_issues:
            report["issues"].append({
                "severity": issue.severity,
                "category": issue.category,
                "contract": issue.contract,
                "function": issue.function,
                "line": issue.line,
                "description": issue.description,
                "recommendation": issue.recommendation
            })
        
        # Save report
        report_path = f"security_audit_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"\n📄 Report saved to: {report_path}")
        
        # Print summary
        print("\n📊 Audit Summary:")
        print("-" * 50)
        print(f"Contracts Analyzed: {self.stats['contracts_analyzed']}")
        print(f"Total Issues: {self.stats['total_issues']}")
        print(f"  🔴 Critical: {self.stats['critical']}")
        print(f"  🟠 High: {self.stats['high']}")
        print(f"  🟡 Medium: {self.stats['medium']}")
        print(f"  🔵 Low: {self.stats['low']}")
        print(f"  ⚪ Info: {self.stats['info']}")
        
        return report


def main():
    parser = argparse.ArgumentParser(description='Smart Contract Security Audit Tool')
    parser.add_argument('contracts_dir', help='Directory containing smart contracts')
    parser.add_argument('--output', '-o', help='Output report file', default='audit_report.json')
    args = parser.parse_args()
    
    if not os.path.exists(args.contracts_dir):
        print(f"❌ Error: Directory {args.contracts_dir} not found")
        sys.exit(1)
    
    auditor = SmartContractAuditor(args.contracts_dir)
    report = auditor.audit_all_contracts()
    
    # Exit with error code if critical issues found
    if report['summary']['critical'] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main() 