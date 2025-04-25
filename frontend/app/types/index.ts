// Employee Types
export interface Employee {
  name: string;
  employee_number: string;
  first_line_support_count: number;
  known_absences: string[];
  metadata: Record<string, any>;
}

export interface EmployeeCreateRequest {
  name: string;
  employee_number: string;
  known_absences?: string[];
  metadata?: Record<string, any>;
}

// Schedule Types
export interface Schedule {
  date: string;
  first_line_support: string;
}

export interface ScheduleCreateRequest {
  date: string;
  first_line_support: string;
}

// Rules Types
export interface Rules {
  max_days_per_week: number;
  preferred_balance: number;
}

export interface RulesUpdateRequest {
  max_days_per_week?: number;
  preferred_balance?: number;
}

export interface ScheduleChange {
  target_date: string;
  suggested_replacement: string;
}

// Schedule Change Types
export interface ScheduleChangeAnalysis {
  thoughts: string;
  original_query: string;
  reason?: string;
  changes: ScheduleChange[];
  recommendation: 'approve' | 'deny' | 'discuss';
  reasoning: string;
}

export interface ScheduleChangeRequest {
  request_text: string;
  metadata?: Record<string, any>;
}

export interface ScheduleChangeResponse {
  request: string;
  analysis: ScheduleChangeAnalysis;
}

// API Response Types
export interface MessageResponse {
  message: string;
}

// UI State Types
export interface ScheduleWithEmployee extends Schedule {
  employee_name: string; // Augmented with employee name for display
}

export interface UIState {
  employees: Employee[];
  schedules: ScheduleWithEmployee[];
  rules: Rules;
  isLoading: boolean;
  error: string | null;
}
