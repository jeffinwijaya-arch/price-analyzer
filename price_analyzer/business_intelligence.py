#!/usr/bin/env python3
"""
Business Intelligence & Analytics Engine
========================================
Advanced analytics, real-time alerts, profit analysis, and business insights.
Provides comprehensive business intelligence for luxury watch dealing operations.
"""

import json
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
from collections import defaultdict, Counter
import statistics
import threading
import time

logger = logging.getLogger(__name__)

@dataclass
class BusinessAlert:
    """Business alert/notification"""
    id: str
    type: str  # lifecycle_gap, stale_inventory, cash_flow, performance, system
    priority: str  # low, normal, high, urgent
    title: str
    message: str
    data: Dict[str, Any]
    created_at: datetime
    expires_at: Optional[datetime] = None
    acknowledged: bool = False
    actions: List[Dict[str, str]] = None
    
    def __post_init__(self):
        if self.actions is None:
            self.actions = []
        if isinstance(self.created_at, str):
            self.created_at = datetime.fromisoformat(self.created_at)
        if isinstance(self.expires_at, str):
            self.expires_at = datetime.fromisoformat(self.expires_at)

@dataclass
class ProfitAnalysis:
    """Profit analysis result"""
    total_revenue: float
    total_cost: float
    gross_profit: float
    gross_margin: float
    net_profit: float
    net_margin: float
    avg_profit_per_watch: float
    profit_by_model: Dict[str, float]
    profit_by_supplier: Dict[str, float]
    profit_by_month: Dict[str, float]
    top_performers: List[Dict[str, Any]]
    underperformers: List[Dict[str, Any]]

@dataclass
class InventoryMetrics:
    """Inventory performance metrics"""
    total_watches: int
    total_value: float
    unsold_count: int
    unsold_value: float
    avg_days_to_sell: float
    turnover_rate: float
    stale_inventory_count: int
    stale_inventory_value: float
    posted_but_unsold: int
    arrived_but_unposted: int

@dataclass
class CashFlowMetrics:
    """Cash flow performance metrics"""
    cash_inflow_30d: float
    cash_outflow_30d: float
    net_cash_flow_30d: float
    outstanding_receivables: float
    outstanding_payables: float
    avg_collection_time: float
    overdue_receivables: float
    working_capital: float

class BusinessIntelligence:
    """Advanced business intelligence and analytics engine"""
    
    def __init__(self):
        self.data_dir = Path("bi_data")
        self.data_dir.mkdir(exist_ok=True)
        
        self.alerts_file = self.data_dir / "alerts.json"
        self.metrics_cache = {}
        self.alerts: Dict[str, BusinessAlert] = {}
        
        # Alert thresholds (configurable)
        self.thresholds = {
            'stale_inventory_days': 30,
            'urgent_collection_days': 30,
            'low_margin_threshold': 0.1,  # 10%
            'high_value_watch': 50000,
            'inventory_turnover_min': 6,  # times per year
            'cash_flow_warning': -10000  # negative cash flow warning
        }
        
        self._load_alerts()
        
        # Start background monitoring
        self.monitoring_active = True
        self.monitoring_thread = threading.Thread(target=self._background_monitoring, daemon=True)
        self.monitoring_thread.start()
    
    def _load_alerts(self):
        """Load alerts from database"""
        if not self.alerts_file.exists():
            return
        
        try:
            with open(self.alerts_file) as f:
                data = json.load(f)
            
            self.alerts = {}
            for alert_id, alert_data in data.items():
                self.alerts[alert_id] = BusinessAlert(**alert_data)
                
        except Exception as e:
            logger.error(f"Failed to load alerts: {e}")
    
    def _save_alerts(self):
        """Save alerts to database"""
        try:
            data = {}
            for alert_id, alert in self.alerts.items():
                alert_dict = asdict(alert)
                # Convert datetime to ISO string
                for key, value in alert_dict.items():
                    if isinstance(value, datetime):
                        alert_dict[key] = value.isoformat()
                data[alert_id] = alert_dict
            
            with open(self.alerts_file, 'w') as f:
                json.dump(data, f, indent=2, default=str)
        except Exception as e:
            logger.error(f"Failed to save alerts: {e}")
    
    def _generate_alert_id(self) -> str:
        """Generate unique alert ID"""
        return f"alert_{int(datetime.now().timestamp())}_{len(self.alerts)}"
    
    def add_alert(self, alert_type: str, priority: str, title: str, 
                  message: str, data: Dict[str, Any] = None,
                  expire_hours: int = 24, actions: List[Dict[str, str]] = None) -> str:
        """Add a new business alert"""
        alert_id = self._generate_alert_id()
        
        expires_at = datetime.now() + timedelta(hours=expire_hours)
        
        alert = BusinessAlert(
            id=alert_id,
            type=alert_type,
            priority=priority,
            title=title,
            message=message,
            data=data or {},
            created_at=datetime.now(),
            expires_at=expires_at,
            actions=actions or []
        )
        
        self.alerts[alert_id] = alert
        self._save_alerts()
        
        logger.info(f"Created {priority} alert: {title}")
        return alert_id
    
    def acknowledge_alert(self, alert_id: str) -> bool:
        """Acknowledge an alert"""
        if alert_id not in self.alerts:
            return False
        
        self.alerts[alert_id].acknowledged = True
        self._save_alerts()
        return True
    
    def get_active_alerts(self, priority: str = None, alert_type: str = None) -> List[BusinessAlert]:
        """Get active (non-acknowledged, non-expired) alerts"""
        now = datetime.now()
        active_alerts = []
        
        for alert in self.alerts.values():
            # Skip acknowledged alerts
            if alert.acknowledged:
                continue
            
            # Skip expired alerts
            if alert.expires_at and now > alert.expires_at:
                continue
            
            # Filter by priority
            if priority and alert.priority != priority:
                continue
            
            # Filter by type
            if alert_type and alert.type != alert_type:
                continue
            
            active_alerts.append(alert)
        
        # Sort by priority and creation time
        priority_order = {'urgent': 0, 'high': 1, 'normal': 2, 'low': 3}
        active_alerts.sort(key=lambda a: (priority_order.get(a.priority, 2), a.created_at), reverse=True)
        
        return active_alerts
    
    def cleanup_old_alerts(self):
        """Remove old acknowledged or expired alerts"""
        now = datetime.now()
        cutoff = now - timedelta(days=7)  # Keep for 7 days
        
        alerts_to_remove = []
        for alert_id, alert in self.alerts.items():
            should_remove = (
                alert.acknowledged and alert.created_at < cutoff
            ) or (
                alert.expires_at and now > alert.expires_at + timedelta(days=1)
            )
            
            if should_remove:
                alerts_to_remove.append(alert_id)
        
        for alert_id in alerts_to_remove:
            del self.alerts[alert_id]
        
        if alerts_to_remove:
            self._save_alerts()
            logger.info(f"Cleaned up {len(alerts_to_remove)} old alerts")
    
    def analyze_lifecycle_gaps(self, inventory_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Analyze inventory for lifecycle gaps"""
        gaps = []
        
        for item in inventory_data:
            ref = item.get('description', 'Unknown')
            sold_to = item.get('sold_to')
            
            # Sold but not shipped
            if (item.get('sold') == 'Yes' and 
                item.get('shipped') != 'Yes' and 
                sold_to):
                gaps.append({
                    'type': 'sold_not_shipped',
                    'ref': ref,
                    'buyer': sold_to,
                    'priority': 'high',
                    'message': f"Need label for {ref} to {sold_to}",
                    'amount': item.get('sold_price', 0)
                })
            
            # Shipped but not paid
            elif (item.get('shipped') == 'Yes' and 
                  item.get('paid_after_sold') != 'Yes' and 
                  sold_to):
                gaps.append({
                    'type': 'shipped_not_paid',
                    'ref': ref,
                    'buyer': sold_to,
                    'priority': 'high',
                    'message': f"{sold_to} owes for {ref}",
                    'amount': item.get('sold_price', 0)
                })
            
            # Not posted (excluding at-store watches)
            elif (item.get('posted') != 'Yes' and 
                  item.get('at_store') != 'Yes' and 
                  not sold_to):
                gaps.append({
                    'type': 'arrived_not_posted',
                    'ref': ref,
                    'priority': 'normal',
                    'message': f"{ref} sitting unposted — idle money",
                    'amount': item.get('cost_price', 0)
                })
        
        return gaps
    
    def analyze_profit_performance(self, inventory_data: List[Dict[str, Any]]) -> ProfitAnalysis:
        """Comprehensive profit analysis"""
        total_revenue = 0
        total_cost = 0
        profit_by_model = defaultdict(float)
        profit_by_supplier = defaultdict(float)
        profit_by_month = defaultdict(float)
        
        completed_sales = []
        
        for item in inventory_data:
            if item.get('sold') == 'Yes' and item.get('sold_price'):
                try:
                    sold_price = float(str(item['sold_price']).replace('$', '').replace(',', ''))
                    cost_price = float(str(item.get('cost_price', 0)).replace('$', '').replace(',', ''))
                    profit = sold_price - cost_price
                    
                    total_revenue += sold_price
                    total_cost += cost_price
                    
                    # Extract model from description
                    desc = item.get('description', '')
                    model_match = desc.split()[0] if desc else 'Unknown'
                    profit_by_model[model_match] += profit
                    
                    # Profit by supplier
                    supplier = item.get('bought_from', 'Unknown')
                    profit_by_supplier[supplier] += profit
                    
                    # Profit by month (if sale date available)
                    sale_date = item.get('sale_date')
                    if sale_date:
                        try:
                            if isinstance(sale_date, str) and sale_date:
                                # Parse various date formats
                                month_key = self._parse_month_from_date(sale_date)
                                if month_key:
                                    profit_by_month[month_key] += profit
                        except:
                            pass
                    
                    completed_sales.append({
                        'ref': desc,
                        'profit': profit,
                        'margin': profit / sold_price if sold_price > 0 else 0,
                        'revenue': sold_price,
                        'cost': cost_price,
                        'supplier': supplier
                    })
                    
                except (ValueError, TypeError):
                    continue
        
        gross_profit = total_revenue - total_cost
        gross_margin = gross_profit / total_revenue if total_revenue > 0 else 0
        
        # Calculate net profit (simplified - would include expenses in real implementation)
        net_profit = gross_profit  # Placeholder
        net_margin = net_profit / total_revenue if total_revenue > 0 else 0
        
        avg_profit = gross_profit / len(completed_sales) if completed_sales else 0
        
        # Top and underperformers
        top_performers = sorted(completed_sales, key=lambda x: x['profit'], reverse=True)[:10]
        underperformers = [sale for sale in completed_sales if sale['margin'] < self.thresholds['low_margin_threshold']]
        
        return ProfitAnalysis(
            total_revenue=total_revenue,
            total_cost=total_cost,
            gross_profit=gross_profit,
            gross_margin=gross_margin,
            net_profit=net_profit,
            net_margin=net_margin,
            avg_profit_per_watch=avg_profit,
            profit_by_model=dict(profit_by_model),
            profit_by_supplier=dict(profit_by_supplier),
            profit_by_month=dict(profit_by_month),
            top_performers=top_performers,
            underperformers=underperformers
        )
    
    def analyze_inventory_metrics(self, inventory_data: List[Dict[str, Any]]) -> InventoryMetrics:
        """Comprehensive inventory analysis"""
        total_watches = len(inventory_data)
        total_value = 0
        unsold_count = 0
        unsold_value = 0
        posted_but_unsold = 0
        arrived_but_unposted = 0
        stale_count = 0
        stale_value = 0
        
        sale_times = []  # For calculating average days to sell
        
        for item in inventory_data:
            try:
                cost_price = float(str(item.get('cost_price', 0)).replace('$', '').replace(',', ''))
                total_value += cost_price
                
                is_sold = item.get('sold') == 'Yes'
                is_posted = item.get('posted') == 'Yes'
                is_arrived = item.get('arrived') == 'Yes'
                
                if not is_sold:
                    unsold_count += 1
                    unsold_value += cost_price
                    
                    if is_posted:
                        posted_but_unsold += 1
                        
                        # Check if stale (posted for > threshold days)
                        # This is simplified - would check actual posting date
                        posted_but_unsold += 1
                
                if not is_posted and not item.get('at_store'):
                    arrived_but_unposted += 1
                
                # Calculate sale time (simplified)
                if is_sold:
                    # Would calculate actual days from purchase to sale
                    # Placeholder: assume 30 days average
                    sale_times.append(30)
                
            except (ValueError, TypeError):
                continue
        
        avg_days_to_sell = statistics.mean(sale_times) if sale_times else 0
        turnover_rate = 365 / avg_days_to_sell if avg_days_to_sell > 0 else 0
        
        return InventoryMetrics(
            total_watches=total_watches,
            total_value=total_value,
            unsold_count=unsold_count,
            unsold_value=unsold_value,
            avg_days_to_sell=avg_days_to_sell,
            turnover_rate=turnover_rate,
            stale_inventory_count=stale_count,
            stale_inventory_value=stale_value,
            posted_but_unsold=posted_but_unsold,
            arrived_but_unposted=arrived_but_unposted
        )
    
    def generate_performance_insights(self, inventory_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Generate actionable business insights"""
        insights = []
        
        profit_analysis = self.analyze_profit_performance(inventory_data)
        inventory_metrics = self.analyze_inventory_metrics(inventory_data)
        
        # Profitability insights
        if profit_analysis.gross_margin < 0.15:  # Less than 15% margin
            insights.append({
                'type': 'profitability',
                'priority': 'high',
                'title': 'Low Overall Margin',
                'message': f'Gross margin is only {profit_analysis.gross_margin:.1%}. Consider focusing on higher-margin models.',
                'action': 'Review pricing strategy and supplier costs'
            })
        
        # Inventory insights
        if inventory_metrics.arrived_but_unposted > 3:
            insights.append({
                'type': 'inventory',
                'priority': 'normal',
                'title': 'Unposted Inventory',
                'message': f'{inventory_metrics.arrived_but_unposted} watches arrived but not posted — idle capital.',
                'action': 'Post watches for sale immediately'
            })
        
        if inventory_metrics.turnover_rate < self.thresholds['inventory_turnover_min']:
            insights.append({
                'type': 'inventory',
                'priority': 'normal',
                'title': 'Slow Inventory Turnover',
                'message': f'Inventory turnover is {inventory_metrics.turnover_rate:.1f}x per year. Target: {self.thresholds["inventory_turnover_min"]}x.',
                'action': 'Review pricing or focus on faster-moving models'
            })
        
        # Top performing models insight
        if profit_analysis.profit_by_model:
            top_model = max(profit_analysis.profit_by_model.items(), key=lambda x: x[1])
            if top_model[1] > 5000:
                insights.append({
                    'type': 'opportunity',
                    'priority': 'normal',
                    'title': 'Focus on High-Profit Model',
                    'message': f'{top_model[0]} generated ${top_model[1]:,.0f} profit. Consider sourcing more.',
                    'action': f'Increase inventory of {top_model[0]} models'
                })
        
        # Underperforming suppliers
        if profit_analysis.underperformers:
            low_margin_count = len(profit_analysis.underperformers)
            insights.append({
                'type': 'supplier',
                'priority': 'normal',
                'title': 'Low-Margin Suppliers',
                'message': f'{low_margin_count} watches sold with <{self.thresholds["low_margin_threshold"]:.0%} margin.',
                'action': 'Review supplier pricing or negotiate better terms'
            })
        
        return insights
    
    def _parse_month_from_date(self, date_str: str) -> Optional[str]:
        """Parse month-year from various date formats"""
        try:
            # Handle various formats: "15 February 2026", "02/17/2026", "2026-02-15", etc.
            if '/' in date_str:
                parts = date_str.split('/')
                if len(parts) >= 2:
                    month = parts[0] if len(parts[0]) <= 2 else parts[1]
                    year = parts[2] if len(parts) > 2 else '2026'
                    return f"{year}-{month.zfill(2)}"
            elif ' ' in date_str:
                # Format like "15 February 2026"
                parts = date_str.split()
                if len(parts) >= 3:
                    month_name = parts[1]
                    year = parts[2]
                    month_num = self._month_name_to_number(month_name)
                    if month_num:
                        return f"{year}-{month_num:02d}"
            elif '-' in date_str:
                # ISO format
                parts = date_str.split('-')
                if len(parts) >= 2:
                    return f"{parts[0]}-{parts[1]}"
        except:
            pass
        return None
    
    def _month_name_to_number(self, month_name: str) -> Optional[int]:
        """Convert month name to number"""
        months = {
            'january': 1, 'february': 2, 'march': 3, 'april': 4, 'may': 5, 'june': 6,
            'july': 7, 'august': 8, 'september': 9, 'october': 10, 'november': 11, 'december': 12,
            'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
            'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
        }
        return months.get(month_name.lower())
    
    def _background_monitoring(self):
        """Background monitoring for real-time alerts"""
        while self.monitoring_active:
            try:
                self.cleanup_old_alerts()
                
                # Periodic monitoring would go here
                # For now, just sleep
                time.sleep(300)  # Check every 5 minutes
                
            except Exception as e:
                logger.error(f"Background monitoring error: {e}")
                time.sleep(60)  # Wait a minute before retrying
    
    def stop_monitoring(self):
        """Stop background monitoring"""
        self.monitoring_active = False
    
    def generate_business_report(self, inventory_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate comprehensive business report"""
        profit_analysis = self.analyze_profit_performance(inventory_data)
        inventory_metrics = self.analyze_inventory_metrics(inventory_data)
        insights = self.generate_performance_insights(inventory_data)
        active_alerts = self.get_active_alerts()
        
        return {
            'generated_at': datetime.now().isoformat(),
            'profit_analysis': asdict(profit_analysis),
            'inventory_metrics': asdict(inventory_metrics),
            'insights': insights,
            'active_alerts': [asdict(alert) for alert in active_alerts],
            'summary': {
                'total_revenue': profit_analysis.total_revenue,
                'gross_profit': profit_analysis.gross_profit,
                'gross_margin': profit_analysis.gross_margin,
                'inventory_value': inventory_metrics.total_value,
                'unsold_value': inventory_metrics.unsold_value,
                'alert_count': len(active_alerts),
                'urgent_alerts': len([a for a in active_alerts if a.priority == 'urgent'])
            }
        }
    
    def get_real_time_metrics(self) -> Dict[str, Any]:
        """Get real-time business metrics for dashboard"""
        active_alerts = self.get_active_alerts()
        
        return {
            'alerts': {
                'total': len(active_alerts),
                'urgent': len([a for a in active_alerts if a.priority == 'urgent']),
                'high': len([a for a in active_alerts if a.priority == 'high']),
                'recent': [asdict(a) for a in active_alerts[:5]]
            },
            'timestamp': datetime.now().isoformat()
        }

# Global BI instance
_bi_instance = None

def get_business_intelligence() -> BusinessIntelligence:
    """Get singleton business intelligence instance"""
    global _bi_instance
    if _bi_instance is None:
        _bi_instance = BusinessIntelligence()
    return _bi_instance

# Convenience functions for dashboard integration
def create_lifecycle_gap_alerts(inventory_data: List[Dict[str, Any]]) -> List[str]:
    """Create alerts for lifecycle gaps"""
    bi = get_business_intelligence()
    gaps = bi.analyze_lifecycle_gaps(inventory_data)
    
    alert_ids = []
    for gap in gaps:
        alert_id = bi.add_alert(
            alert_type='lifecycle_gap',
            priority=gap['priority'],
            title=f"Action needed: {gap['type'].replace('_', ' ').title()}",
            message=gap['message'],
            data=gap,
            actions=[
                {'label': 'Create Label', 'action': 'create_label'},
                {'label': 'Send Reminder', 'action': 'send_reminder'}
            ] if gap['type'] in ['sold_not_shipped', 'shipped_not_paid'] else []
        )
        alert_ids.append(alert_id)
    
    return alert_ids

def get_dashboard_analytics(inventory_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Get analytics data for dashboard"""
    bi = get_business_intelligence()
    return bi.generate_business_report(inventory_data)

def check_business_health(inventory_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Quick business health check"""
    bi = get_business_intelligence()
    
    profit_analysis = bi.analyze_profit_performance(inventory_data)
    inventory_metrics = bi.analyze_inventory_metrics(inventory_data)
    active_alerts = bi.get_active_alerts()
    
    # Simple health score calculation
    health_score = 100
    
    if profit_analysis.gross_margin < 0.15:
        health_score -= 20
    if inventory_metrics.arrived_but_unposted > 5:
        health_score -= 15
    if len([a for a in active_alerts if a.priority == 'urgent']) > 0:
        health_score -= 25
    if inventory_metrics.turnover_rate < 6:
        health_score -= 10
    
    health_score = max(0, health_score)
    
    status = 'excellent' if health_score >= 90 else 'good' if health_score >= 70 else 'warning' if health_score >= 50 else 'critical'
    
    return {
        'health_score': health_score,
        'status': status,
        'gross_margin': profit_analysis.gross_margin,
        'inventory_turnover': inventory_metrics.turnover_rate,
        'urgent_alerts': len([a for a in active_alerts if a.priority == 'urgent']),
        'idle_inventory': inventory_metrics.arrived_but_unposted,
        'timestamp': datetime.now().isoformat()
    }

if __name__ == "__main__":
    # Test the business intelligence system
    logging.basicConfig(level=logging.INFO)
    bi = get_business_intelligence()
    print("Business intelligence ready!")