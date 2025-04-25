import axios from 'axios';
import type {
  Employee,
  EmployeeCreateRequest,
  Schedule,
  ScheduleCreateRequest,
  Rules,
  RulesUpdateRequest,
  ScheduleChangeRequest,
  ScheduleChangeResponse,
  MessageResponse
} from '~/types';

const API_URL = `${import.meta.env.VITE_API_BASE_URL || 'http://localhost:3000'}/api`;

// Create request/response logger
const logRequest = (config: any) => {
  console.log(`Request: ${config.method?.toUpperCase()} ${config.url}`, config.data || '');
  return config;
};

const logResponse = (response: any) => {
  console.log(`Response: ${response.status} ${response.config.url}`, response.data);
  return response;
};

const logError = (error: any) => {
  console.error('API Error:', error.response?.status, error.response?.data || error.message);
  return Promise.reject(error);
};

const api = axios.create({
  baseURL: API_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Add request/response logging
api.interceptors.request.use(logRequest, logError);
api.interceptors.response.use(logResponse, logError);

// Employee API calls
export const fetchEmployees = async (): Promise<Employee[]> => {
  const response = await api.get<Employee[]>('/employees');
  return response.data;
};

export const fetchEmployee = async (employeeNumber: string): Promise<Employee> => {
  const response = await api.get<Employee>(`/employees/${employeeNumber}`);
  return response.data;
};

export const createEmployee = async (employee: EmployeeCreateRequest): Promise<Employee> => {
  const response = await api.post<Employee>('/employees', employee);
  return response.data;
};

export const updateEmployee = async (employeeNumber: string, employee: EmployeeCreateRequest): Promise<Employee> => {
  const response = await api.put<Employee>(`/employees/${employeeNumber}`, employee);
  return response.data;
};

export const deleteEmployee = async (employeeNumber: string): Promise<MessageResponse> => {
  const response = await api.delete<MessageResponse>(`/employees/${employeeNumber}`);
  return response.data;
};

// Schedule API calls
export const fetchSchedules = async (startDate?: string, endDate?: string): Promise<Schedule[]> => {
  const params = new URLSearchParams();
  if (startDate) params.append('start_date', startDate);
  if (endDate) params.append('end_date', endDate);

  const url = params.toString() ? `/schedules?${params.toString()}` : '/schedules';
  const response = await api.get<Schedule[]>(url);
  return response.data;
};

export const fetchSchedule = async (date: string): Promise<Schedule> => {
  const response = await api.get<Schedule>(`/schedules/${date}`);
  return response.data;
};

export const createSchedule = async (schedule: ScheduleCreateRequest): Promise<Schedule> => {
  const response = await api.post<Schedule>('/schedules', schedule);
  return response.data;
};

export const updateSchedule = async (date: string, schedule: ScheduleCreateRequest): Promise<Schedule> => {
  const response = await api.put<Schedule>(`/schedules/${date}`, schedule);
  return response.data;
};

export const deleteSchedule = async (date: string): Promise<MessageResponse> => {
  const response = await api.delete<MessageResponse>(`/schedules/${date}`);
  return response.data;
};

// Rules API calls
export const fetchRules = async (): Promise<Rules> => {
  const response = await api.get<Rules>('/rules');
  return response.data;
};

export const updateRules = async (rules: RulesUpdateRequest): Promise<Rules> => {
  const response = await api.put<Rules>('/rules', rules);
  return response.data;
};

// Schedule Change API calls
export const processScheduleChange = async (request: ScheduleChangeRequest): Promise<ScheduleChangeResponse> => {
  const response = await api.post<ScheduleChangeResponse>('/schedule-changes', request);
  return response.data;
};