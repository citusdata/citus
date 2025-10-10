#!/usr/bin/env python3
"""
Backward compatibility checker for Citus
Detects changes that could break existing workflows
"""

import os
import re
import json
import subprocess
from pathlib import Path
from dataclasses import dataclass
from typing import List


@dataclass
class FunctionSignature:
    name: str
    args: dict[dict]
    return_type: str
    
    def __init__(self, name: str, args: str, return_type: str):
        self.name = name
        self.args = self._parse_parameters(args)
        self.return_type = return_type

    def __str__(self):
        return f"{self.name}({self.args}) -> {self.return_type}"
    
    def compare(self, other: 'FunctionSignature') -> dict:
        """Compare two function signatures and return differences"""
        if not isinstance(other, FunctionSignature):
            return {"error": "Cannot compare with non-FunctionSignature object"}
        
        differences = []
        
        if self.name != other.name:
            differences.append(f"Function name changed from {self.name} to {other.name}.")

        if self.return_type != other.return_type:
            differences.append(f"Return type changed from {self.return_type} to {other.return_type}.")

        arg_diff = self._compare_parameters(other.args)
        if arg_diff:
            differences.append(f"Parameter changes detected:\n{arg_diff}")

        return '\n'.join(differences) if differences else None

    def _parse_parameters(self, args_string: str) -> dict[dict]:
        """Parse parameter string into structured data"""
        if not args_string.strip():
            return {}

        params = {}
        for param in args_string.split(','):
            param = param.strip()
            if param:
                default_value = None
                # Extract name, type, and default
                if 'DEFAULT' in param.upper():
                    # Capture the default value (everything after DEFAULT up to a comma or closing parenthesis)
                    m = re.search(r'\bDEFAULT\b\s+([^,)\s][^,)]*)', param, flags=re.IGNORECASE)
                    if m:
                        default_value = m.group(1).strip()
                    # Remove the DEFAULT clause from the parameter string for further parsing
                    param_clean = re.sub(r'\s+DEFAULT\s+[^,)]+', '', param, flags=re.IGNORECASE)
                else:
                    param_clean = param
                
                parts = param_clean.strip().split()
                if len(parts) >= 2:
                    name = parts[0]
                    type_part = ' '.join(parts[1:])
                else:
                    name = param_clean
                    type_part = ""
                
                params[name] = {
                    'type': type_part,
                    'default_value': default_value,
                    'original': param
                }
        
        return params

    def _compare_parameters(self, new_params: dict[dict]) -> dict:
        """Compare parameter lists"""
        differences = []
        # Removed parameters
        removed = set(self.args.keys()) - set(new_params.keys())
        added = set(new_params.keys()) - set(self.args.keys())

        added_without_default = []
        for name in added:
            if new_params[name]['default_value'] is None:
                added_without_default.append(name)

        # Find modified parameters
        type_changed = []
        default_changed = []
        for name, old_param in self.args.items():
            if name in new_params:
                new_param = new_params[name]
                if old_param['type'] != new_param['type']:
                    type_changed.append(name)
                if old_param['default_value'] and old_param['default_value'] != new_param['default_value']:
                    default_changed.append(name)

        if removed:
            differences.append(f"Removed parameters: {', '.join(removed)}")
        if added_without_default:
            differences.append(f"Added parameters without a default value: {', '.join(added_without_default)}")
        if type_changed:
            differences.append(f"Type changed for parameters: {', '.join(type_changed)}")
        if default_changed:
            differences.append(f"Default value changed for parameters: {', '.join(default_changed)}")

        return '\n'.join(differences) if differences else None


class CompatibilityChecker:
    def __init__(self):
        self.base_sha = os.environ.get('BASE_SHA')
        self.head_sha = os.environ.get('HEAD_SHA')
        self.results = []

    def get_changed_files(self):
        """Get list of changed files between base and head"""
        cmd = ['git', 'diff', '--name-only', f'{self.base_sha}...{self.head_sha}']
        result = subprocess.run(cmd, capture_output=True, text=True)
        return result.stdout.strip().split('\n') if result.stdout.strip() else []

    def get_file_diff(self, file_path):
        """Get diff for a specific file"""
        cmd = ['git', 'diff', f'{self.base_sha}...{self.head_sha}', '--', file_path]
        result = subprocess.run(cmd, capture_output=True, text=True)
        return result.stdout
    
    def get_base_file_content(self, file_path):
        """Get file content from base commit"""
        cmd = ['git', 'show', f'{self.base_sha}:{file_path}']
        result = subprocess.run(cmd, capture_output=True, text=True)
        return result.stdout if result.returncode == 0 else ''

    def get_head_file_content(self, file_path):
        """Get file content from head commit"""
        cmd = ['git', 'show', f'{self.head_sha}:{file_path}']
        result = subprocess.run(cmd, capture_output=True, text=True)
        return result.stdout if result.returncode == 0 else ''
    
    def get_function_signatures(self, sql_text: str) -> List[FunctionSignature]:
        """Extract all function signatures from SQL text"""
        pattern = re.compile(
            r'CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+'
            r'(?P<name>[^\s(]+)\s*'         # function name, e.g. public.add_numbers
            r'\((?P<args>[^)]*)\)'          # argument list
            r'\s*RETURNS\s+(?P<return>(?:SETOF\s+)?(?:TABLE\s*\([^)]+\)|[\w\[\]]+(?:\s*\[\s*\])*))',  # return type
            re.IGNORECASE | re.MULTILINE
        )
        matches = pattern.finditer(sql_text)
        return [FunctionSignature(match.group('name'), match.group('args'), match.group('return')) for match in matches]

    def check_sql_migrations(self, changed_files):
        """Check for potentially breaking SQL migration changes"""
        breaking_patterns = [
            (r'DROP\s+TABLE', 'Table removal'),
            (r'ALTER\s+TABLE\s+pg_catalog\.\w+\s+(ADD|DROP)\s+COLUMN\b', 'Column addition/removal in pg_catalog'),
            (r'ALTER\s+TABLE\s+\w+\s+ALTER\s+COLUMN', 'Column type change'),
            (r'ALTER\s+FUNCTION.*RENAME', 'Function rename'),
            (r'ALTER\s+TABLE\s+\w+\s+RENAME\s+TO\s+\w+', 'Table rename'),
            (r'REVOKE', 'Permission revocation')
        ]
        
        upgrade_scripts = [f for f in changed_files if 'sql' in f and '/downgrades/' not in f and 'citus--' in f]

        udf_files = [f for f in changed_files if f.endswith('latest.sql')]

        for file_path in upgrade_scripts:
            diff = self.get_file_diff(file_path)
            added_lines = [line[1:] for line in diff.split('\n') if line.startswith('+')]

            for pattern, description in breaking_patterns:
                for line in added_lines:
                    if re.search(pattern, line, re.IGNORECASE):
                        self.results.append({
                            'type': 'SQL Migration',
                            'description': description,
                            'file': file_path,
                            'details': f'Line: {line.strip()}'
                        })

        for file_path in udf_files:
            udf_directory = Path(file_path).parent.name
            base_content = self.get_base_file_content(file_path)
            if not base_content:
                continue  # File did not exist in base, likely a new file
            head_content = self.get_head_file_content(file_path)
            if not head_content:
                self.results.append({
                    'type': 'UDF Removal',
                    'description': f'UDF file removed: {udf_directory}',
                    'file': file_path,
                    'details': 'The UDF file is missing in the new version'
                })
                continue

            # Extract function signatures from base and head
            base_functions = self.get_function_signatures(base_content)
            head_functions = self.get_function_signatures(head_content)

            if not base_functions or not head_functions:
                continue  # Could not parse function signatures

            for base_function in base_functions:
                found = False
                differences = None
                for head_function in head_functions:
                    differences = base_function.compare(head_function)
                    if not differences:
                        found = True
                        break  # Found one for the previous signature
                if not found and differences:
                    self.results.append({
                        'type': 'UDF Change',
                        'description': f'UDF changed: {udf_directory}',
                        'file': file_path,
                        'details': differences
                    })

    def check_guc_changes(self, changed_files):
        """Check for GUC (configuration) changes"""
        c_files = [f for f in changed_files if f.endswith('shared_library_init.c')]
        if not c_files:
            return
        
        file_path = c_files[0] # There should be only one shared_library_init.c

        guc_pattern = re.compile(
            r'^-\s*DefineCustom\w+Variable\s*\(\s*\n'     # DefineCustom...Variable  (removed)
            r'\s*-\s*"([^"]+)"',                      # Parameter name removed
            re.MULTILINE
        )

        diff = self.get_file_diff(file_path)
        for match in guc_pattern.finditer(diff):
            print("Matched GUC line:", match.group(0))
            guc_name = match.group(1)
            self.results.append({
                'type': 'Configuration',
                'description': f'GUC change: {guc_name}',
                'file': file_path,
                'details': 'GUC removed'
            })


    def run_checks(self):
        """Run all compatibility checks"""
        changed_files = self.get_changed_files()
        
        if not changed_files:
            print("No changed files found")
            return
        
        print(f"Checking {len(changed_files)} changed files...")
        
        self.check_sql_migrations(changed_files)
        self.check_guc_changes(changed_files)
        
        # Write results to file for GitHub Action to read
        with open('/tmp/compat-results.json', 'w') as f:
            json.dump(self.results, f, indent=2)
        
        # Print summary
        total_issues = len(self.results)

        if total_issues > 0:
            print(f"\n  Found {total_issues} potential compatibility issues:")
            for issue in self.results:
                print(f"  - {issue['description']} in {issue['file']}")
        else:
            print("\n No backward compatibility issues detected")

if __name__ == '__main__':
    checker = CompatibilityChecker()
    checker.run_checks()