---
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

# USER STORY: Real-Time Portfolio Risk Analytics Dashboard

| **Attribute** | **Details** |
|---------------|-------------|
| **Story ID** | SPARK-FINTECH-001 |
| **Title** | Real-Time Portfolio Risk Analytics Dashboard |
| **Created By** | Product Management Team |
| **Date Created** | 2024-01-15 |
| **Last Updated** | 2024-01-15 |
| **Priority** | High |
| **Epic/Initiative** | Financial Services Analytics Platform |
| **Story Points** | 21 (Complex) |
| **Target Release** | Spark 4.1.0 |
| **Stakeholders** | Portfolio Managers, Risk Management Team, Trading Desk Supervisors, Compliance Officers |

---

## Section 1: Business Context

### 1.1 Problem Statement

**Current State:**
Investment management firms currently rely on batch-processed risk reports that are generated on scheduled intervals (typically end-of-day or hourly during market hours). These reports introduce significant latency between actual portfolio positions and risk visibility, creating several critical challenges:

- **Decision Latency**: Portfolio managers lack real-time awareness of risk exposures during active trading periods, preventing timely responses to market movements
- **Risk Blind Spots**: Intraday position changes and market volatility can create concentration risks that remain invisible until the next scheduled report run
- **Operational Inefficiency**: Manual refresh requests and ad-hoc risk calculations consume valuable analyst time and computational resources
- **Regulatory Concerns**: Delayed risk visibility increases the probability of breaching regulatory concentration limits or internal risk thresholds
- **Competitive Disadvantage**: Firms with real-time risk visibility can execute more agile portfolio adjustments and capitalize on market opportunities more effectively

**Quantified Impact:**
- Average decision latency of 30-60 minutes from position change to risk visibility
- 15-20% of risk threshold breaches are detected only in retrospective reports
- Risk analysts spend approximately 25% of their time running manual recalculations
- Estimated opportunity cost of $2-5M annually from delayed decision-making in volatile markets

**Business Problem:**
The absence of real-time portfolio risk analytics creates an unacceptable gap between portfolio actions and risk awareness, undermining effective risk management and strategic decision-making during critical market hours.

### 1.2 User Persona

#### Primary Persona: Senior Portfolio Manager

**Profile:**
- **Name**: Sarah Chen
- **Role**: Senior Portfolio Manager - Equity Strategies
- **Experience**: 12 years in portfolio management, CFA charter holder
- **Portfolio Size**: $2.5B AUM across 350-450 positions
- **Technology Proficiency**: High - comfortable with Bloomberg Terminal, proprietary trading systems, and analytics dashboards

**Responsibilities:**
- Execute investment strategy within defined risk parameters
- Monitor portfolio exposures and rebalance as market conditions change
- Ensure compliance with concentration limits (sector, geographic, single-issuer)
- Respond to client inquiries about portfolio risk profile
- Collaborate with trading desk on optimal execution timing

**Pain Points:**
- Cannot see real-time impact of trades on portfolio-level risk metrics
- Relies on end-of-day reports to validate that positions remain within risk budgets
- Misses intraday opportunities to reduce concentrations at favorable prices
- Spends significant time manually calculating approximate risk impacts before making decisions

**Goals:**
- Monitor portfolio VaR continuously during market hours to ensure risk budget compliance
- Identify concentration buildups immediately as new positions are added
- Assess correlation impacts of position changes in real-time
- Make data-driven rebalancing decisions with sub-minute information latency

**Success Metrics:**
- Reduce average time from trade execution to risk visibility from 45 minutes to <1 second
- Decrease risk threshold breaches by 60% through proactive monitoring
- Increase portfolio efficiency (return per unit of risk) by 8-12% through better intraday risk management

#### Secondary Persona: Risk Management Officer

**Profile:**
- **Name**: Michael Rodriguez
- **Role**: Risk Management Officer
- **Experience**: 15 years in risk management across investment banking and asset management
- **Scope**: Enterprise-wide risk oversight for $15B+ in assets across multiple strategies
- **Technology Proficiency**: Advanced - experienced with risk management platforms, quantitative analytics, and data visualization

**Responsibilities:**
- Monitor aggregate risk exposures across all portfolio strategies
- Ensure compliance with firm-wide risk limits and regulatory requirements
- Generate risk reports for executive management and board committees
- Investigate and document risk limit exceptions
- Conduct stress testing and scenario analysis

**Pain Points:**
- Limited visibility into intraday risk developments across portfolios
- Cannot provide real-time guidance to portfolio managers on risk budget availability
- Risk reporting to senior management relies on stale data
- Manual investigation of limit breaches is time-consuming and reactive

**Goals:**
- Real-time dashboard view of firm-wide risk exposures aggregated across strategies
- Automated alerting when portfolios approach risk thresholds
- Historical trending capability to identify patterns in risk utilization
- Drill-down capability from aggregate to position-level risk contributors

**Success Metrics:**
- Reduce time to identify and investigate risk exceptions from 2 hours to <5 minutes
- Achieve 99.5% uptime for risk monitoring during market hours
- Decrease false-positive risk alerts by 40% through better calculation accuracy

#### Tertiary Persona: Trading Desk Supervisor

**Profile:**
- **Name**: James Patterson
- **Role**: Head Trader - Equity Desk
- **Experience**: 10 years as institutional trader
- **Daily Volume**: 5,000-8,000 trades across 20+ portfolios
- **Technology Proficiency**: Expert - deeply familiar with order management systems, execution algorithms

**Responsibilities:**
- Coordinate trade execution for multiple portfolio managers
- Optimize execution timing to minimize market impact
- Manage trading desk workflow and resource allocation
- Provide feedback to PMs on liquidity and execution feasibility

**Pain Points:**
- Receives rebalancing requests without understanding urgency from risk perspective
- Cannot prioritize trades based on real-time risk reduction impact
- Lacks visibility into which trades will have most significant portfolio risk effects

**Goals:**
- Understand risk-driven trade priorities in real-time
- Coordinate with PMs using shared real-time risk view
- Execute risk-reducing trades with appropriate urgency

**Success Metrics:**
- Reduce average time from PM trade request to execution by 20%
- Improve trade prioritization accuracy based on risk impact

### 1.3 Business Objectives

**Primary Objective:**
Enable real-time visibility into portfolio risk exposures during market hours (9:30 AM - 4:00 PM ET) to support rapid, data-driven investment decisions and proactive risk management.

**Strategic Goals:**

1. **Operational Excellence**
   - Reduce decision-making latency from position changes to risk visibility by 99% (from 30-60 minutes to <1 second)
   - Eliminate manual risk calculation requests, freeing 25% of risk analyst capacity for strategic analysis
   - Enable portfolio managers to execute 3-5x more intraday portfolio adjustments with confidence

2. **Risk Management Enhancement**
   - Decrease frequency of risk limit breaches by 60% through proactive monitoring
   - Reduce time to detect and respond to concentration buildups from hours to seconds
   - Improve risk-adjusted returns (Sharpe ratio) by 8-12% through better intraday position management

3. **Regulatory Compliance**
   - Maintain continuous monitoring for SEC-mandated concentration limits
   - Provide comprehensive audit trail of risk exposures and threshold breaches
   - Generate real-time compliance reports for regulatory inquiries

4. **Competitive Advantage**
   - Match or exceed risk technology capabilities of top-quartile investment firms
   - Enable more sophisticated trading strategies that require real-time risk awareness
   - Attract and retain quantitative portfolio managers who demand advanced analytics

**Key Performance Indicators (KPIs):**

| KPI | Current State | Target State | Measurement Method |
|-----|---------------|--------------|-------------------|
| Risk Metric Refresh Latency | 30-60 minutes (batch) | <1 second | p95 latency from position update to dashboard display |
| System Availability (Market Hours) | 95% (scheduled reports) | 99.5% | Uptime monitoring during 9:30 AM - 4:00 PM ET |
| Risk Limit Breaches (Proactive Detection) | 40% detected intraday | 95% detected intraday | Audit log analysis of breach timestamps vs. occurrence |
| Portfolio Manager Adoption | N/A (no real-time system) | 90% daily active users | Usage analytics - daily logins during market hours |
| Manual Risk Calculation Requests | 50-80 per day | <10 per day | Help desk ticket volume analysis |
| Time to Risk Investigation | 2 hours average | <5 minutes average | Timestamp analysis from alert to resolution |

### 1.4 Success Criteria

**Must-Have Requirements (MVP):**

- [ ] Dashboard displays portfolio-level Value at Risk (VaR) with <1 second refresh latency
- [ ] Sector concentration metrics update in real-time as positions change
- [ ] System supports portfolios with up to 500 positions without performance degradation
- [ ] Dashboard accessible to all authorized users during full market hours (9:30 AM - 4:00 PM ET)
- [ ] Authentication and role-based access control for sensitive portfolio data
- [ ] Basic alerting when risk metrics exceed predefined thresholds
- [ ] Historical comparison showing current risk vs. previous day/week
- [ ] Mobile-responsive interface accessible from tablets

**Should-Have Requirements (Post-MVP):**

- [ ] Geographic concentration exposure with country/region breakdown
- [ ] Beta and correlation calculations against major market indices
- [ ] Drill-down from portfolio level to position-level risk contribution
- [ ] Customizable dashboard layouts and metric preferences per user
- [ ] Export capability for risk snapshots (PDF/Excel)
- [ ] Integration with compliance monitoring systems for automated breach reporting

**Could-Have Requirements (Future Enhancements):**

- [ ] Predictive analytics showing projected risk under various scenarios
- [ ] Machine learning-based anomaly detection for unusual risk patterns
- [ ] Multi-portfolio comparison views for risk officers
- [ ] Integration with trading systems for trade impact simulation ("what-if" analysis)
- [ ] Mobile native applications (iOS/Android) beyond responsive web

**Acceptance Criteria (Detailed):**

1. **Performance**
   - 95th percentile latency for dashboard refresh: <1000ms
   - 99th percentile latency for dashboard refresh: <2000ms
   - Time to first meaningful paint (TTFMP) on page load: <3000ms
   - Support 100+ concurrent users without performance degradation

2. **Accuracy**
   - Risk calculations match independently verified results within 0.1% tolerance
   - Position data freshness: <5 second lag from source system updates
   - Market data freshness: <10 second lag from exchange feeds

3. **Reliability**
   - System uptime during market hours: 99.5% (allows ~1.5 hours downtime per month)
   - Graceful degradation: display last known values with staleness indicator if feeds fail
   - Automatic recovery from transient data source failures within 60 seconds

4. **Usability**
   - New users can access their portfolio dashboard within 5 minutes of onboarding
   - 90% of users rate interface as "easy to use" or "very easy to use" in UX surveys
   - Zero critical accessibility violations (WCAG 2.1 AA compliance)

5. **Security**
   - All portfolio data encrypted in transit (TLS 1.3) and at rest (AES-256)
   - Audit log captures all user actions and data access with tamper-proof storage
   - Failed authentication attempts locked out after 5 tries

### 1.5 Scope and Boundaries

**In Scope:**

- Real-time calculation and display of the following risk metrics:
  - Portfolio Value at Risk (VaR) - 1-day, 95% confidence, Historical Simulation method
  - Sector concentration exposure (top 10 sectors by percentage of portfolio)
  - Largest position exposures (top 20 positions by market value and risk contribution)
  - Cash position and gross/net leverage ratios
- Web-based dashboard interface accessible via modern browsers (Chrome, Firefox, Safari, Edge)
- Integration with existing position management system via REST API
- Integration with market data provider for real-time price feeds
- User authentication via corporate SSO (SAML 2.0)
- Role-based access control (Portfolio Manager, Risk Officer, Trader, Read-Only)
- Basic threshold-based alerting via dashboard notifications
- Historical data retention (30 days of intraday snapshots, 2 years of end-of-day)

**Out of Scope (Not in Initial Release):**

- Advanced VaR methodologies (Parametric, Monte Carlo) - deferred to Phase 2
- Options Greeks and derivatives risk analytics - separate initiative
- Integration with order management systems for pre-trade risk checks
- Custom report generation and scheduling - use existing reporting platform
- Mobile native applications - mobile-responsive web only in Phase 1
- Email/SMS alerting - dashboard notifications only initially
- Multi-currency portfolio consolidation - USD-only initially
- Fixed income specific risk metrics (duration, convexity, spread risk)

**Assumptions:**

- Position management system provides REST API with <1 second response time
- Market data provider delivers real-time price updates via WebSocket or similar streaming protocol
- Corporate SSO infrastructure supports SAML 2.0 integration
- Portfolio managers have modern workstations with browsers capable of WebSocket connections
- Network connectivity between risk dashboard and data sources is reliable (99.9% uptime)
- Existing data warehouse can be queried for historical position and price data for VaR lookback window

**Dependencies:**

- **Data Source Systems:**
  - Position Management System (PMS) - provides current holdings, cost basis, quantities
  - Market Data Provider - delivers real-time prices for all securities in portfolios
  - Reference Data System - provides security master data, sector classifications, identifiers
  - Corporate SSO/Identity Provider - SAML authentication service

- **Infrastructure:**
  - Cloud or on-premise infrastructure for deployment (AWS, Azure, or internal data center)
  - Database system for time-series storage (TimescaleDB, InfluxDB, or similar)
  - Message broker for real-time data streaming (Apache Kafka or Pulsar)
  - API gateway for secure external access

- **Organizational:**
  - Risk Management team defines risk threshold values and alert rules
  - Compliance team reviews and approves audit logging approach
  - IT Security team conducts security assessment and penetration testing
  - Portfolio Manager representatives participate in UAT and provide feedback

**Constraints:**

- **Technical Constraints:**
  - Must integrate with existing PMS without requiring vendor modifications
  - Must operate within firm's approved technology stack (Java/Python/Scala backend, React/Angular/Vue frontend)
  - Must comply with network security policies (no direct internet access from production systems)
  - Database storage limited to 10TB for Phase 1 (accommodates ~2 years historical data)

- **Business Constraints:**
  - Phase 1 budget: $750K (development) + $150K (infrastructure first year)
  - Phase 1 timeline: 6 months from kickoff to production launch
  - Implementation must not disrupt current end-of-day risk reporting processes
  - Requires approval from Chief Risk Officer and Chief Information Officer

- **Regulatory Constraints:**
  - Must maintain SOX controls for financial data accuracy and audit trails
  - Must comply with SEC Rule 17a-4 for electronic records retention
  - User access logs must be retained for 7 years in immutable storage
  - System must support regulatory examination requests (data extraction capability)

---

## Section 2: Functional Requirements

### 2.1 Core Features

#### Feature 1: Real-Time Portfolio Risk Dashboard

**Description:**
Primary web-based interface displaying key risk metrics for a selected portfolio with automatic refresh as underlying position and market data changes.

**User Story:**
As a Portfolio Manager, I want to see my portfolio's current risk metrics updated in real-time so that I can make informed decisions about position adjustments during market hours without waiting for scheduled reports.

**Functional Details:**

**Dashboard Components:**
1. **Portfolio Selector**
   - Dropdown or search interface to select portfolio from user's authorized list
   - Display portfolio name, strategy type, and total AUM
   - Support for favoriting frequently accessed portfolios

2. **Key Metrics Summary Panel**
   - Large, prominent display of critical metrics:
     - Portfolio VaR (1-day, 95% confidence) in USD and % of NAV
     - Gross Leverage ratio
     - Net Leverage ratio
     - Cash Position (USD and % of portfolio)
     - Largest Position (% of portfolio)
   - Color coding: Green (within thresholds), Yellow (approaching limits), Red (breach)
   - Timestamp of last update with age indicator

3. **Sector Concentration Chart**
   - Horizontal bar chart or treemap showing top 10 sectors by portfolio weight
   - Comparison bars showing sector limits if applicable
   - Drill-down capability to see positions within each sector

4. **Top Positions Table**
   - Table showing top 20 positions by market value
   - Columns: Symbol, Name, Quantity, Market Value, % Portfolio, Price, Price Change % Today
   - Sortable by any column
   - Click to drill into position details

5. **Historical Trend Chart**
   - Line chart showing VaR and leverage trending over selected time period (1 day, 1 week, 1 month)
   - Comparison of current values to recent history
   - Identification of historical peaks and troughs

6. **Alert Panel**
   - List of active alerts (risk threshold breaches or approaching limits)
   - Severity indicators (Info, Warning, Critical)
   - Timestamp and brief description of alert condition
   - Acknowledge/dismiss capability

**Acceptance Criteria:**
- [ ] Dashboard loads completely within 3 seconds for portfolios up to 500 positions
- [ ] Metrics update within 1 second of position change in source system
- [ ] All charts and tables render correctly at common screen resolutions (1920x1080, 1366x768, 2560x1440)
- [ ] Dashboard remains responsive during rapid market movements (high volatility periods)
- [ ] User can switch between portfolios with <2 second transition time
- [ ] Timestamp displays current Eastern Time and updates with each refresh

#### Feature 2: Value at Risk (VaR) Calculation Engine

**Description:**
Real-time computation of 1-day 95% Value at Risk using Historical Simulation methodology, recalculated automatically as positions or prices change.

**User Story:**
As a Risk Management Officer, I want the system to calculate portfolio VaR in real-time using a consistent, transparent methodology so that I can monitor risk budget utilization throughout the trading day.

**Functional Details:**

**VaR Calculation Methodology:**
- **Method**: Historical Simulation (non-parametric)
- **Confidence Level**: 95% (5th percentile of loss distribution)
- **Time Horizon**: 1-day
- **Lookback Period**: 252 trading days (approximately 1 year)
- **Return Calculation**: Logarithmic returns for each position
- **Position Weighting**: Current market values applied to historical return scenarios

**Calculation Steps:**
1. Retrieve current portfolio positions (symbol, quantity, current price, market value)
2. Retrieve 252 days of historical daily closing prices for all positions
3. Calculate daily log returns for each position: `r_t = ln(P_t / P_{t-1})`
4. For each historical scenario (day), compute hypothetical portfolio return:
   - `Portfolio Return_scenario = Σ(w_i * r_i_scenario)` where w_i is current position weight
5. Sort 252 scenario returns from worst to best
6. Identify 5th percentile return (252 * 0.05 = 12.6, round to 13th worst scenario)
7. Convert percentile return to dollar VaR: `VaR = -1 * Portfolio_Value * Return_5th_percentile`

**Optimization Strategy:**
- Pre-compute historical return matrix for all securities in reference universe
- Incremental recalculation: only recompute VaR when position weights change materially (>0.1% of portfolio)
- Cache VaR results with 10-second TTL if positions unchanged
- Parallel computation for multi-portfolio scenarios

**Data Requirements:**
- Input: Current position data (symbol, quantity, price, market value)
- Input: Historical daily prices for all securities (252 trading days)
- Output: Portfolio VaR in USD
- Output: VaR as percentage of portfolio NAV
- Output: Calculation timestamp and data freshness indicators

**Acceptance Criteria:**
- [ ] VaR calculation completes in <500ms for portfolios up to 500 positions
- [ ] VaR results match independent verification calculations within 0.1% tolerance
- [ ] System handles missing historical data gracefully (exclude positions with insufficient history, flag warning)
- [ ] VaR recalculates automatically within 1 second of position change >0.1% of portfolio
- [ ] Historical price data refreshed daily before market open
- [ ] System logs all VaR calculations with timestamp, input data version, and result for audit purposes

#### Feature 3: Concentration Risk Analysis

**Description:**
Real-time aggregation and monitoring of portfolio exposures by sector, geography, and individual issuer to identify concentration risks.

**User Story:**
As a Portfolio Manager, I want to see my portfolio's concentration by sector and issuer in real-time so that I can ensure I'm not exceeding concentration limits and maintain proper diversification.

**Functional Details:**

**Concentration Dimensions:**

1. **Sector Concentration**
   - Aggregate market value by GICS sector classification (11 sectors)
   - Calculate percentage of portfolio for each sector
   - Compare to sector limits (if defined in portfolio policy)
   - Highlight sectors exceeding thresholds (e.g., >20% in single sector)
   - Display top 3 positions within each significant sector

2. **Issuer Concentration**
   - Aggregate market value by issuer (using parent company mapping for subsidiaries)
   - Identify top 20 issuers by portfolio weight
   - Flag issuers exceeding single-issuer limits (e.g., >5% of portfolio per SEC Rule 15c3-1 for certain fund types)
   - Track number of issuers representing 50% of portfolio (concentration measure)

3. **Geographic Concentration** (Post-MVP)
   - Aggregate by country of domicile or primary exchange
   - Calculate percentage exposure to each country/region
   - Display on geographic heat map visualization

**Threshold-Based Alerting:**
- Define threshold rules per portfolio:
  - Example: "Alert if any single sector >25% of portfolio"
  - Example: "Alert if any single issuer >5% of portfolio"
  - Example: "Alert if top 10 positions >40% of portfolio"
- Generate alert when threshold crossed (from below to above)
- Clear alert when concentration falls back below threshold with hysteresis (e.g., must drop to <23% to clear 25% sector alert)

**Data Requirements:**
- Input: Position data with security identifiers
- Input: Reference data mapping securities to GICS sectors, issuers, countries
- Input: Portfolio policy limits for concentration thresholds
- Output: Sector concentration table (sector, market value, % portfolio, vs. limit)
- Output: Top issuer concentration table (issuer, market value, % portfolio, positions count)
- Output: Active concentration alerts list

**Acceptance Criteria:**
- [ ] Concentration analysis updates within 1 second of position changes
- [ ] Sector classifications are accurate for 99%+ of positions (based on reference data quality)
- [ ] Alerts trigger immediately when thresholds are crossed (no delay)
- [ ] System handles positions without sector/issuer mapping gracefully (group as "Unclassified" with warning)
- [ ] Concentration calculations are additive and sum to 100% of portfolio (accounting validation)
- [ ] Historical concentration trending available for 30-day lookback

#### Feature 4: Market Sensitivity Metrics (Beta and Correlation)

**Description:**
Calculate and display portfolio beta relative to major market indices and correlation between portfolio returns and benchmark returns, updated in real-time.

**User Story:**
As a Portfolio Manager, I want to understand my portfolio's sensitivity to market movements (beta) and how closely it tracks my benchmark (correlation) so that I can assess whether my active positions are generating the intended risk-return profile.

**Functional Details:**

**Beta Calculation:**
- **Definition**: Beta measures portfolio's volatility relative to market index
- **Formula**: β = Cov(R_portfolio, R_market) / Var(R_market)
- **Rolling Window**: 60 trading days (approximately 3 months)
- **Benchmark Indices**: 
  - S&P 500 (SPX) for US large-cap equity portfolios
  - Russell 2000 (RUT) for US small-cap equity portfolios
  - MSCI World (MXWO) for global equity portfolios
  - Custom benchmark as defined in portfolio policy

**Calculation Approach:**
1. Calculate daily portfolio returns for last 60 trading days using historical positions and prices
2. Retrieve daily benchmark index returns for same period
3. Compute covariance between portfolio and benchmark returns
4. Compute variance of benchmark returns
5. Beta = Covariance / Variance

**Interpretation Guidance:**
- Beta = 1.0: Portfolio moves in line with market
- Beta > 1.0: Portfolio more volatile than market (amplified movements)
- Beta < 1.0: Portfolio less volatile than market (dampened movements)
- Beta < 0: Portfolio moves inverse to market (hedge/short portfolio)

**Correlation Calculation:**
- **Definition**: Correlation measures strength of linear relationship between portfolio and benchmark returns
- **Formula**: ρ = Cov(R_portfolio, R_market) / (σ_portfolio * σ_market)
- **Range**: -1.0 to +1.0
- **Rolling Window**: 60 trading days

**Display Format:**
- Numeric display of beta with 2 decimal precision (e.g., "Beta: 1.15")
- Numeric display of correlation with 2 decimal precision (e.g., "Correlation: 0.87")
- Trend chart showing beta and correlation over time (1 month, 3 months, 6 months, 1 year)
- Comparison to policy targets if defined (e.g., "Target Beta: 0.9-1.1")

**Update Frequency:**
- Recalculate daily after market close when new returns are available
- Display last calculated value during intraday (beta/correlation are inherently backward-looking metrics)
- Flag as "Stale" if calculation is >1 trading day old

**Acceptance Criteria:**
- [ ] Beta and correlation calculations match manual Excel calculations within 0.01
- [ ] Metrics update within 15 minutes of market close each trading day
- [ ] Trend charts display correctly with appropriate date range selection
- [ ] System allows selection of different benchmark indices for comparison
- [ ] Warning displayed if insufficient historical data for reliable calculation (<30 days)

#### Feature 5: Cash and Leverage Monitoring

**Description:**
Real-time calculation and display of portfolio cash position, gross leverage, and net leverage ratios.

**User Story:**
As a Risk Management Officer, I want to monitor portfolio leverage ratios in real-time to ensure portfolios operate within their authorized leverage limits and maintain adequate cash buffers.

**Functional Details:**

**Metric Definitions:**

1. **Cash Position**
   - **Formula**: Sum of all cash and cash-equivalent positions
   - **Display**: USD amount and percentage of total portfolio value
   - **Includes**: Bank deposits, money market funds, T-bills <90 days maturity
   - **Example**: "$15.2M cash (6.1% of portfolio)"

2. **Gross Leverage**
   - **Formula**: (Total Long Market Value + |Total Short Market Value|) / Portfolio NAV
   - **Interpretation**: Measures total capital deployed including both long and short positions
   - **Example**: Gross Leverage = 1.5x means portfolio has $1.50 of exposure for every $1.00 of NAV
   - **Display**: Ratio format with 2 decimal places (e.g., "1.45x")

3. **Net Leverage**
   - **Formula**: (Total Long Market Value - |Total Short Market Value|) / Portfolio NAV
   - **Interpretation**: Measures net directional exposure (net long or net short)
   - **Example**: Net Leverage = 1.0x means portfolio is 100% net long (fully invested)
   - **Display**: Ratio format with 2 decimal places (e.g., "0.95x")

**Calculation Components:**
- **Total Long Market Value**: Sum of market value for all positions where quantity > 0
- **Total Short Market Value**: Sum of market value for all positions where quantity < 0 (expressed as positive number for leverage calculation)
- **Portfolio NAV**: Total assets - total liabilities (provided by position management system)

**Threshold Monitoring:**
- Compare to portfolio-specific leverage limits (e.g., "Gross Leverage Limit: 2.0x")
- Alert if leverage exceeds limit or cash position falls below minimum (e.g., "Cash minimum: 3%")
- Color-coded indicators: Green (within limits), Yellow (>90% of limit), Red (exceeding limit)

**Acceptance Criteria:**
- [ ] Cash, gross leverage, and net leverage update within 1 second of position changes
- [ ] Leverage calculations handle short positions correctly (absolute value in gross, negative in net)
- [ ] Alert triggers within 1 second when leverage exceeds defined threshold
- [ ] Historical trending shows leverage utilization over past 30 days
- [ ] Calculations verified against portfolio accounting system end-of-day values with <0.1% variance

#### Feature 6: User Authentication and Authorization

**Description:**
Secure user authentication via corporate Single Sign-On (SSO) and role-based access control to ensure users can only access portfolios and data they are authorized to view.

**User Story:**
As a System Administrator, I want to control user access to sensitive portfolio data through integration with our corporate identity system so that we maintain proper data security and comply with information access policies.

**Functional Details:**

**Authentication:**
- **Method**: SAML 2.0 integration with corporate Identity Provider (IdP)
- **Login Flow**:
  1. User accesses dashboard URL
  2. System redirects to corporate SSO login page
  3. User authenticates with corporate credentials (username/password + MFA if required)
  4. IdP returns SAML assertion with user identity and group memberships
  5. System validates assertion, creates session, grants access to dashboard

**Authorization Model:**

**Roles:**
1. **Portfolio Manager**
   - Can view all portfolios they are assigned to manage
   - Can view detailed position data and risk metrics
   - Can acknowledge/dismiss alerts for their portfolios
   - Can customize dashboard layouts and preferences
   - Cannot view other managers' portfolios (data separation)

2. **Risk Officer**
   - Can view all portfolios across the firm (firm-wide access)
   - Can view detailed risk metrics and drill-downs
   - Can generate ad-hoc risk reports
   - Can configure risk thresholds and alert rules
   - Read-only access to position data (cannot modify)

3. **Trader**
   - Can view portfolios relevant to their trading desk
   - Can see position exposures and key risk metrics
   - Limited drill-down capability (top-level metrics only)
   - Cannot configure thresholds or access audit logs

4. **Compliance Officer**
   - Can view all portfolios (firm-wide access)
   - Can access audit logs and user activity reports
   - Can view historical risk data for investigations
   - Read-only access (cannot modify any data or configurations)

5. **Read-Only / Executive Viewer**
   - Can view high-level summary metrics for authorized portfolios
   - No drill-down capability to position details
   - Dashboard view only (no data export)

**Portfolio-Level Access Control:**
- User-to-portfolio mapping maintained in centralized authorization database
- Access granted based on:
  - Direct assignment: User explicitly granted access to specific portfolio
  - Group membership: User's AD group grants access to portfolio group
  - Role-based: Risk Officers get access to all portfolios automatically

**Session Management:**
- Session timeout: 60 minutes of inactivity
- Absolute session timeout: 12 hours (requires re-authentication)
- Concurrent session limit: 3 sessions per user
- Secure session tokens (httpOnly, secure, sameSite attributes)

**Audit Logging:**
- Log all authentication attempts (successful and failed)
- Log all portfolio access (user, portfolio, timestamp)
- Log all data exports and report generation
- Log all configuration changes (thresholds, alerts)
- Logs retained for 7 years in tamper-proof storage

**Acceptance Criteria:**
- [ ] SSO authentication completes successfully with corporate IdP for 100 test users
- [ ] User can only access portfolios they are explicitly authorized for
- [ ] Unauthorized access attempts are blocked and logged
- [ ] Session expires correctly after inactivity timeout
- [ ] User role changes in AD propagate to dashboard within 5 minutes
- [ ] All authentication and authorization events logged with complete details
- [ ] SAML assertion signature validation prevents token tampering

### 2.2 User Workflows

#### Workflow 1: Morning Portfolio Risk Review

**Actor:** Portfolio Manager (Sarah Chen)

**Trigger:** Start of trading day (9:15 AM ET, 15 minutes before market open)

**Preconditions:**
- Portfolio Manager is authenticated via SSO
- Position data reflects end-of-day positions from previous day plus any pre-market adjustments
- Market data feeds are operational

**Workflow Steps:**

1. **Login and Dashboard Access** (1 minute)
   - Navigate to risk dashboard URL
   - Automatically redirected to corporate SSO
   - Authenticate with credentials + MFA token
   - System displays portfolio selector screen

2. **Select Primary Portfolio** (30 seconds)
   - Select "US Large Cap Growth Strategy" from portfolio dropdown
   - Dashboard loads showing overnight risk metrics

3. **Review Key Metrics** (2 minutes)
   - Check Portfolio VaR: "$4.2M VaR (1.68% of NAV)" - within $5M limit ✓
   - Review Gross Leverage: "1.15x" - within 1.5x limit ✓
   - Check Cash Position: "$22M (8.8%)" - above 5% minimum ✓
   - Note: All metrics green (within thresholds)

4. **Review Sector Concentrations** (2 minutes)
   - Examine sector bar chart
   - Note: Technology sector at 28.5% (approaching 30% limit) - Yellow warning
   - Identify largest tech positions: MSFT (5.2%), AAPL (4.8%), NVDA (3.9%)
   - Decision: Monitor tech sector closely, consider trimming if market rises

5. **Check for Alerts** (1 minute)
   - Alert panel shows 1 warning: "Technology sector at 95% of limit"
   - Click alert for details: Crossed 95% threshold yesterday at 2:45 PM
   - Acknowledge alert (marked as reviewed)

6. **Review Top Positions** (2 minutes)
   - Scan top 20 positions table
   - Note: No single position >5% of portfolio ✓
   - Largest position: MSFT at 5.2% ($13.0M)
   - Review price changes: Most positions flat in pre-market

7. **Historical Trend Review** (2 minutes)
   - Switch trend chart to 1-week view
   - Observe: VaR has increased from $3.8M to $4.2M over past week (market volatility increasing)
   - Gross leverage stable around 1.12-1.15x
   - Note: Risk trending upward, stay vigilant

8. **Document Observations** (3 minutes)
   - Open trading journal (separate system)
   - Document key observations: "Tech concentration near limit, VaR elevated vs last week, monitor for trim opportunities"
   - Set mental note: If NVDA rallies >3% today, consider partial trim to reduce tech weight

**Postconditions:**
- Portfolio Manager has complete awareness of overnight risk position
- Identified potential action items for trading day
- Documented observations for decision audit trail

**Total Time:** ~14 minutes

**Success Metrics:**
- Time to complete morning review reduced from 30 minutes (manual process with Excel) to 15 minutes
- 100% of risk limit compliance checked systematically (vs. ~70% with manual process)
- Documented observations for all portfolios before market open

#### Workflow 2: Intraday Position Change Impact Monitoring

**Actor:** Portfolio Manager (Sarah Chen)

**Trigger:** Placing order to add new position or adjust existing position

**Preconditions:**
- Portfolio Manager has risk dashboard open on secondary monitor
- Live market data flowing
- About to execute order: Buy 5,000 shares GOOGL at market (~$2.5M position)

**Workflow Steps:**

1. **Pre-Trade Risk Check** (30 seconds)
   - Glance at dashboard showing current portfolio state
   - Current VaR: $4.2M (within $5M limit, $800K buffer)
   - Current Tech sector: 28.5% (within 30% limit, 1.5% buffer)
   - GOOGL classification: Technology sector

2. **Execute Trade** (2 minutes)
   - Place order through trading system
   - Order filled: 5,000 shares GOOGL @ $145.50 avg = $727,500

3. **Monitor Real-Time Risk Update** (10 seconds)
   - Position management system receives trade confirmation
   - Dashboard refreshes automatically within 1 second
   - NEW VaR: $4.35M (+$150K, +3.6%) - still within limit ✓
   - NEW Tech sector: 29.8% (+1.3%) - now at 99% of 30% limit - Yellow alert

4. **Evaluate Risk Impact** (1 minute)
   - VaR increase of $150K is acceptable (within expected range for $727K position)
   - Tech concentration now very close to limit (29.8% vs 30%)
   - Alert appears: "Technology sector at 99% of limit"

5. **Decision Point** (2 minutes)
   - Option A: Proceed with current plan (monitor tech closely rest of day)
   - Option B: Proactively trim different tech position to create buffer
   - Decision: Select Option A - accept proximity to limit, will trim if any tech position rallies significantly

6. **Document Decision** (1 minute)
   - Note in trading journal: "Added GOOGL $727K, tech sector now 29.8% (very close to 30% limit), watching for trim opportunity"
   - Acknowledge alert in dashboard

**Postconditions:**
- Portfolio Manager has immediate visibility into risk impact of trade
- Informed decision made about whether additional action needed
- Alert acknowledged to prevent alert fatigue

**Total Time:** ~7 minutes (most time spent on trading system, not risk dashboard)

**Success Metrics:**
- Risk impact visible <1 second after trade confirmation (vs. 45 minutes with batch system)
- Portfolio Manager confidence in staying within risk limits increased
- Reduction in post-trade "surprises" when batch risk reports run

#### Workflow 3: Risk Threshold Breach Investigation

**Actor:** Risk Management Officer (Michael Rodriguez)

**Trigger:** Automated alert indicating a portfolio has breached a risk limit

**Preconditions:**
- Risk Officer logged into dashboard with firm-wide access
- Alert notification appears: "EMEA Equity Portfolio: VaR exceeded limit"

**Workflow Steps:**

1. **Receive and Assess Alert** (30 seconds)
   - Alert notification: "EMEA Equity Portfolio exceeded VaR limit: $8.2M vs $8.0M limit"
   - Timestamp: 10:47:23 AM ET
   - Severity: Critical (red)
   - Click notification to navigate to portfolio

2. **Load Portfolio Dashboard** (5 seconds)
   - Dashboard loads EMEA Equity Portfolio
   - VaR prominently displayed in red: "$8.2M VaR (3.28% of NAV) - EXCEEDS LIMIT"
   - Limit indicator: "$8.0M limit (breached by $200K / 2.5%)"

3. **Investigate Root Cause** (3 minutes)
   - Check historical trend chart: VaR was $7.6M at market open, increased throughout morning
   - Review position changes: See recent trades in dashboard activity log (if available) or check separate trading system
   - Identify: Large position added in volatile European bank stock (Deutsche Bank) at 10:30 AM
   - Check market conditions: European markets showing elevated volatility today (financial sector stress)

4. **Assess Breach Severity** (2 minutes)
   - Breach magnitude: $200K over $8M limit = 2.5% over limit (relatively modest)
   - Duration: Breached 17 minutes ago
   - Market conditions: Volatility spike likely temporary (news-driven, not systematic)
   - Portfolio Manager communication: Check if PM is aware

5. **Contact Portfolio Manager** (5 minutes)
   - Call or IM Portfolio Manager for EMEA Equity
   - Confirm awareness of breach
   - PM response: "Yes, aware. Added Deutsche Bank position this morning taking advantage of dislocation. Expect volatility to settle. Can trim position this afternoon if VaR doesn't naturally decline."
   - Agree on action plan: Monitor for next 2 hours. If VaR still >$8M at 1:00 PM, PM will trim position to bring back within limit.

6. **Document Breach** (5 minutes)
   - Create breach incident record in risk system
   - Document: Breach details, root cause (new volatile position), PM communication, action plan, expected resolution time
   - Set follow-up reminder for 1:00 PM to verify resolution
   - Notify Chief Risk Officer via daily breach summary (end of day report)

7. **Monitor Resolution** (Ongoing)
   - Keep EMEA Equity dashboard visible in monitoring screen
   - Check VaR every 15 minutes
   - At 12:15 PM: VaR dropped to $7.9M (back within limit as volatility settled) - breach self-resolved
   - Update breach incident: "Resolved - VaR returned to $7.9M as market volatility declined. No position trim required."

**Postconditions:**
- Breach identified and investigated within 5 minutes
- Action plan established with Portfolio Manager
- Incident documented for compliance and reporting
- Breach resolved same day

**Total Time:** ~15 minutes active time, plus ongoing monitoring

**Success Metrics:**
- Time from breach to PM notification: <5 minutes (vs. 2+ hours with batch system)
- Breach resolution rate: >90% resolved same day
- Documented action plans for 100% of breaches

### 2.3 Business Rules

#### BR-1: Value at Risk (VaR) Calculation Rules

**Rule ID:** BR-VaR-001  
**Rule Statement:** Portfolio VaR must be calculated using Historical Simulation method with 252-day lookback period, 1-day time horizon, and 95% confidence level.

**Rationale:** Historical Simulation is transparent, non-parametric (no distribution assumptions), and widely accepted by regulators. 252-day lookback provides full year of market scenarios including various market regimes.

**Implementation Logic:**
```
IF portfolio has positions
  THEN
    RETRIEVE 252 days of historical prices for all positions
    CALCULATE daily log returns for each position
    FOR each historical scenario (day 1 to 252):
      CALCULATE hypothetical portfolio return = Σ(current_weight_i * historical_return_i)
    SORT portfolio returns from worst to best
    IDENTIFY 5th percentile return (13th worst scenario out of 252)
    VAR_USD = -1 * Portfolio_NAV * Return_5th_percentile
    VAR_PERCENT = -1 * Return_5th_percentile * 100
  ELSE
    VAR_USD = 0
    VAR_PERCENT = 0
```

**Edge Cases:**
- **Insufficient historical data**: If position has <30 days of price history, exclude from VaR calculation and flag warning
- **Corporate actions**: Adjust historical prices for splits, dividends to maintain continuity
- **New positions**: For positions added today, include in VaR using available price history or proxy (sector ETF returns if <10 days history)

**Validation:** VaR_USD must be positive number, VaR_PERCENT must be between 0% and 100%. If outside range, flag calculation error.

---

#### BR-2: Sector Concentration Threshold Rules

**Rule ID:** BR-CONC-001  
**Rule Statement:** Sector concentration limits are portfolio-specific and defined in portfolio investment policy. Default limit is 25% per sector if no specific limit defined.

**Threshold Levels:**
- **Green (OK)**: Sector weight ≤ 80% of limit
- **Yellow (Warning)**: Sector weight > 80% of limit AND ≤ 100% of limit
- **Red (Breach)**: Sector weight > 100% of limit

**Example:**
- Portfolio limit: 25% per sector
- Green: ≤ 20% (25% * 0.80)
- Yellow: 20.01% to 25%
- Red: > 25%

**Hysteresis:** Once Yellow or Red alert triggered, must fall below 75% of limit to clear (prevent alert flapping)

**Implementation Logic:**
```
FOR each sector in portfolio:
  sector_weight_pct = (sector_market_value / portfolio_NAV) * 100
  sector_limit_pct = GET limit from portfolio_policy WHERE sector = current_sector
  IF sector_limit_pct is NULL THEN sector_limit_pct = 25.0  // default
  
  IF sector_weight_pct > sector_limit_pct THEN
    status = "RED"
    alert = "ACTIVE - Sector " + sector_name + " exceeds limit: " + sector_weight_pct + "% vs " + sector_limit_pct + "%"
  ELSE IF sector_weight_pct > (sector_limit_pct * 0.80) THEN
    status = "YELLOW"
    alert = "WARNING - Sector " + sector_name + " approaching limit: " + sector_weight_pct + "% vs " + sector_limit_pct + "%"
  ELSE
    status = "GREEN"
    alert = NULL
```

---

#### BR-3: Leverage Limit Enforcement Rules

**Rule ID:** BR-LEV-001  
**Rule Statement:** Portfolio gross leverage and net leverage must not exceed portfolio-specific limits as defined in investment policy.

**Standard Limits (if not otherwise specified):**
- Long-only portfolios: Gross Leverage ≤ 1.10x, Net Leverage 0.95x to 1.05x
- Long/Short portfolios: Gross Leverage ≤ 2.00x, Net Leverage -0.30x to +0.30x
- Market-neutral portfolios: Gross Leverage ≤ 3.00x, Net Leverage -0.10x to +0.10x

**Calculation Rules:**
```
gross_leverage = (total_long_mv + ABS(total_short_mv)) / portfolio_NAV
net_leverage = (total_long_mv - ABS(total_short_mv)) / portfolio_NAV

IF gross_leverage > gross_leverage_limit THEN
  trigger_alert("Gross leverage breach", gross_leverage, gross_leverage_limit)
  status = "RED"
  
IF net_leverage > net_leverage_upper_limit OR net_leverage < net_leverage_lower_limit THEN
  trigger_alert("Net leverage breach", net_leverage, net_leverage_limit_range)
  status = "RED"
```

**Grace Period:** Intraday breaches allowed for up to 2 hours if caused by market price movements (not new trades). Must be resolved by end of day.

**Exception Handling:** Leverage calculation fails gracefully if NAV = 0 or NAV is negative (display error message, do not show misleading leverage ratio)

---

#### BR-4: Data Freshness and Staleness Rules

**Rule ID:** BR-DATA-001  
**Rule Statement:** Dashboard must indicate data freshness and warn users when displaying stale data that may not reflect current reality.

**Freshness Thresholds:**

| Data Type | Real-Time Threshold | Stale Threshold | Action if Stale |
|-----------|---------------------|-----------------|-----------------|
| Position Data | < 10 seconds | > 60 seconds | Display yellow "stale" indicator with last update timestamp |
| Market Prices | < 15 seconds | > 60 seconds | Display yellow "stale" indicator with last update timestamp |
| Risk Calculations | < 5 seconds after input change | > 30 seconds after input change | Display yellow "calculating..." indicator |
| Historical Prices (for VaR) | Updated daily before market open | > 2 trading days old | Display red "data outdated" warning, disable VaR display |

**Implementation Logic:**
```
FOR each data element displayed:
  last_update_timestamp = GET timestamp of last data update
  current_time = GET current system time
  data_age_seconds = current_time - last_update_timestamp
  
  IF data_age_seconds > STALE_THRESHOLD THEN
    display_indicator = "STALE - Last updated " + FORMAT_TIME_AGO(last_update_timestamp)
    indicator_color = "YELLOW" or "RED" (depending on severity)
  ELSE IF data_age_seconds > REALTIME_THRESHOLD THEN
    display_indicator = "Updated " + FORMAT_TIME_AGO(last_update_timestamp)
    indicator_color = "GRAY"
  ELSE
    display_indicator = "Live" or "Real-time"
    indicator_color = "GREEN"
```

**User Notification:** If data becomes stale, display banner notification: "Real-time data feed interrupted. Displaying last known values. Attempting to reconnect..."

---

#### BR-5: User Access and Data Security Rules

**Rule ID:** BR-SEC-001  
**Rule Statement:** Users may only access portfolio data for portfolios they are explicitly authorized to view based on their role and portfolio assignments.

**Authorization Check:**
```
FUNCTION check_user_authorization(user_id, portfolio_id):
  user_role = GET user role from identity system
  
  IF user_role == "RISK_OFFICER" OR user_role == "COMPLIANCE_OFFICER" THEN
    RETURN TRUE  // Risk and Compliance have firm-wide access
  
  user_portfolios = GET authorized portfolio list for user_id
  IF portfolio_id IN user_portfolios THEN
    RETURN TRUE
  
  user_groups = GET AD group memberships for user_id
  portfolio_groups = GET authorized groups for portfolio_id
  IF ANY(user_groups) IN portfolio_groups THEN
    RETURN TRUE
  
  RETURN FALSE  // Access denied
```

**Audit Logging Rule:**
- Every portfolio access logged: `LOG(timestamp, user_id, portfolio_id, action="VIEW", ip_address, session_id)`
- Every failed access attempt logged: `LOG(timestamp, user_id, portfolio_id, action="ACCESS_DENIED", ip_address)`
- Logs written to append-only tamper-proof storage
- Logs retained for 7 years per regulatory requirements

**Data Masking Rule:**
- Users with "Read-Only" role see masked position details (security names visible, but quantities and precise market values rounded/masked)
- Example: Instead of "10,000 shares @ $145.50 = $1,455,000", display "~10K shares, ~$1.5M"

### 2.4 Data Requirements

#### Input Data Sources

**Data Source 1: Position Management System**

| Attribute | Details |
|-----------|---------|
| **System Name** | Enterprise Position Management System (EPMS) |
| **Integration Method** | REST API (primary), Database query (backup) |
| **Update Frequency** | Real-time (event-driven updates on position changes) |
| **Expected Latency** | < 5 seconds from trade confirmation to API availability |
| **Data Format** | JSON over HTTPS |
| **Authentication** | API key + OAuth 2.0 client credentials |

**Data Schema:**
```json
{
  "portfolio_id": "string (unique identifier)",
  "portfolio_name": "string",
  "as_of_timestamp": "ISO 8601 datetime",
  "nav": "decimal (total net asset value in USD)",
  "positions": [
    {
      "position_id": "string",
      "security_id": "string (CUSIP, ISIN, or internal ID)",
      "security_name": "string",
      "quantity": "decimal (positive for long, negative for short)",
      "price": "decimal (current market price per share/unit)",
      "market_value": "decimal (quantity * price)",
      "cost_basis": "decimal (original purchase price)",
      "currency": "string (ISO 4217 currency code, e.g., USD)"
    }
  ]
}
```

**API Endpoints:**
- `GET /api/v1/portfolios/{portfolio_id}/positions` - Retrieve current positions
- `GET /api/v1/portfolios?user_id={user_id}` - List portfolios authorized for user
- `WebSocket wss://epms.company.com/api/v1/stream` - Subscribe to real-time position updates

**Data Quality Expectations:**
- Completeness: 100% of positions included (no partial snapshots)
- Accuracy: Position quantities and prices match official books and records
- Timeliness: Updates available within 5 seconds of trade settlement in trading system

---

**Data Source 2: Market Data Provider**

| Attribute | Details |
|-----------|---------|
| **System Name** | Bloomberg Market Data Feed (or equivalent: Refinitiv, IEX Cloud) |
| **Integration Method** | Streaming API (WebSocket or FIX protocol) |
| **Update Frequency** | Real-time tick-by-tick (for subscribed securities) |
| **Expected Latency** | < 100ms from exchange to delivery |
| **Data Format** | Proprietary binary format (Bloomberg BPIPE) or JSON (IEX Cloud) |
| **Authentication** | API key or Bloomberg terminal subscription |

**Data Schema (Simplified JSON representation):**
```json
{
  "security_id": "string (ticker symbol, CUSIP, or ISIN)",
  "timestamp": "ISO 8601 datetime with microseconds",
  "bid_price": "decimal",
  "ask_price": "decimal",
  "last_price": "decimal",
  "last_size": "integer (shares in last trade)",
  "volume": "integer (cumulative daily volume)",
  "change_pct": "decimal (% change from previous close)"
}
```

**Subscription Model:**
- Subscribe to real-time prices for all securities in active portfolios (typically 500-2000 unique securities across all portfolios)
- Dynamic subscription management: Add securities when positions added, remove when no longer held in any portfolio

**Data Quality Expectations:**
- Latency: 95th percentile latency < 500ms from exchange timestamp to receipt
- Coverage: 99.9% of US-listed equities, 98% of international equities
- Uptime: 99.95% during market hours

---

**Data Source 3: Reference Data System**

| Attribute | Details |
|-----------|---------|
| **System Name** | Enterprise Reference Data Master (ERDM) |
| **Integration Method** | REST API (daily batch load) + API for real-time lookups |
| **Update Frequency** | Daily batch (overnight), real-time API for on-demand lookups |
| **Expected Latency** | < 100ms for API lookups |
| **Data Format** | JSON or XML |
| **Authentication** | API key |

**Data Schema:**
```json
{
  "security_id": "string (primary identifier)",
  "identifiers": {
    "cusip": "string",
    "isin": "string",
    "ticker": "string",
    "sedol": "string"
  },
  "descriptive": {
    "security_name": "string",
    "issuer_name": "string",
    "asset_class": "string (EQUITY, FIXED_INCOME, DERIVATIVE, etc.)",
    "security_type": "string (COMMON_STOCK, PREFERRED_STOCK, CORP_BOND, etc.)"
  },
  "classifications": {
    "gics_sector": "string (11 GICS sectors)",
    "gics_industry_group": "string",
    "gics_industry": "string",
    "country_of_risk": "string (ISO 3166 country code)",
    "country_of_domicile": "string",
    "primary_exchange": "string (MIC code)"
  },
  "issuer": {
    "parent_company_id": "string (for concentration aggregation)",
    "parent_company_name": "string"
  }
}
```

**Data Quality Expectations:**
- Coverage: 99%+ of securities have sector classification
- Accuracy: >99.5% accuracy in sector and issuer mappings (validated quarterly)
- Freshness: Corporate actions (name changes, ticker changes) updated within 1 business day

---

**Data Source 4: Historical Price Database**

| Attribute | Details |
|-----------|---------|
| **System Name** | Enterprise Data Warehouse - Historical Prices table |
| **Integration Method** | Direct database connection (read-only) |
| **Update Frequency** | Daily (prices loaded overnight after market close) |
| **Expected Latency** | N/A (batch load) - Available by 7:00 AM ET for previous day |
| **Data Format** | Relational database table (PostgreSQL or similar) |
| **Authentication** | Database credentials (service account) |

**Data Schema:**
```sql
CREATE TABLE historical_prices (
  security_id VARCHAR(50) NOT NULL,
  price_date DATE NOT NULL,
  open_price DECIMAL(18,6),
  high_price DECIMAL(18,6),
  low_price DECIMAL(18,6),
  close_price DECIMAL(18,6),  -- Adjusted for splits/dividends
  volume BIGINT,
  adjustment_factor DECIMAL(18,10),  -- Cumulative adjustment for splits/dividends
  PRIMARY KEY (security_id, price_date)
);

CREATE INDEX idx_historical_prices_date ON historical_prices(price_date);
CREATE INDEX idx_historical_prices_security ON historical_prices(security_id);
```

**Query Pattern for VaR Calculation:**
```sql
SELECT 
  security_id,
  price_date,
  close_price,
  LAG(close_price) OVER (PARTITION BY security_id ORDER BY price_date) AS previous_close,
  LN(close_price / LAG(close_price) OVER (PARTITION BY security_id ORDER BY price_date)) AS log_return
FROM historical_prices
WHERE security_id IN ('list_of_securities_in_portfolio')
  AND price_date >= CURRENT_DATE - INTERVAL '252 trading days'
ORDER BY security_id, price_date;
```

**Data Quality Expectations:**
- Completeness: >99.5% of trading days present for liquid securities
- Accuracy: Prices match exchange official closing prices (validated via third-party data provider)
- Adjustment accuracy: Corporate action adjustments applied correctly (splits, dividends, spinoffs)

#### Output Data / Dashboard Display

**Dashboard Data Model:**

**Primary Dashboard View:**
```json
{
  "portfolio_summary": {
    "portfolio_id": "string",
    "portfolio_name": "string",
    "as_of_timestamp": "ISO 8601 datetime",
    "nav_usd": "decimal",
    "num_positions": "integer"
  },
  "key_metrics": {
    "var_1day_95_usd": "decimal",
    "var_1day_95_pct": "decimal",
    "var_vs_limit": {
      "limit_usd": "decimal",
      "utilization_pct": "decimal",
      "status": "GREEN|YELLOW|RED"
    },
    "gross_leverage": "decimal",
    "net_leverage": "decimal",
    "cash_position_usd": "decimal",
    "cash_position_pct": "decimal"
  },
  "sector_concentration": [
    {
      "sector_name": "string (GICS sector)",
      "market_value_usd": "decimal",
      "pct_of_portfolio": "decimal",
      "limit_pct": "decimal",
      "num_positions": "integer",
      "status": "GREEN|YELLOW|RED"
    }
  ],
  "top_positions": [
    {
      "security_id": "string",
      "security_name": "string",
      "ticker": "string",
      "quantity": "decimal",
      "price": "decimal",
      "market_value_usd": "decimal",
      "pct_of_portfolio": "decimal",
      "change_pct_today": "decimal"
    }
  ],
  "alerts": [
    {
      "alert_id": "string",
      "severity": "INFO|WARNING|CRITICAL",
      "message": "string",
      "timestamp": "ISO 8601 datetime",
      "acknowledged": "boolean"
    }
  ],
  "data_freshness": {
    "position_data_age_seconds": "integer",
    "market_data_age_seconds": "integer",
    "risk_calculation_timestamp": "ISO 8601 datetime"
  }
}
```

---

## Section 3: Technical Requirements

### 3.1 System Architecture

#### 3.1.1 High-Level Architecture

**Architecture Style:** Modern microservices architecture with event-driven data ingestion and real-time streaming processing.

**Architectural Layers:**

1. **Presentation Layer (Frontend)**
   - Technology: React.js with TypeScript
   - Real-time Updates: WebSocket connection to backend for live data push
   - State Management: Redux or Zustand for client-side state
   - Charting: D3.js or Recharts for data visualization

2. **API Gateway Layer**
   - Technology: Spring Cloud Gateway or Kong API Gateway
   - Responsibilities: Authentication, rate limiting, request routing, SSL termination
   - Security: JWT token validation, API key management

3. **Application Services Layer (Backend)**
   - Technology: Java/Spring Boot or Scala/Akka (leveraging Spark ecosystem)
   - Services:
     - **Portfolio Service**: Manages portfolio metadata, user authorization
     - **Risk Calculation Service**: Executes VaR, concentration, leverage calculations
     - **Market Data Service**: Manages market data subscriptions and caching
     - **Alert Service**: Threshold monitoring and alert generation
     - **User Service**: Authentication, authorization, session management

4. **Data Processing Layer**
   - Technology: Apache Spark Structured Streaming for real-time position and price processing
   - Responsibilities: Ingest position updates, market data ticks, transform to standardized format, trigger risk recalculations

5. **Data Storage Layer**
   - **Time-Series Database**: TimescaleDB or InfluxDB for storing historical risk metrics, position snapshots
   - **Relational Database**: PostgreSQL for user data, portfolio metadata, configuration
   - **Cache Layer**: Redis for sub-second access to current positions, prices, calculated risk metrics
   - **Historical Data Warehouse**: PostgreSQL or Snowflake for historical price data (252+ days)

6. **Integration Layer**
   - **Message Broker**: Apache Kafka for event streaming (position updates, price ticks)
   - **ETL Pipelines**: Nightly batch jobs to load historical prices, reference data updates

#### 3.1.2 Data Flow Architecture

**Real-Time Risk Update Flow:**

```
1. Position Change Event:
   [Trading System] --> Trade Execution
         ↓
   [Position Management System] --> Position Update Event
         ↓
   [Kafka Topic: position-updates] --> Event Published
         ↓
   [Spark Structured Streaming] --> Consume Event, Transform
         ↓
   [Redis Cache] --> Update Current Position Cache
         ↓
   [Risk Calculation Service] --> Trigger VaR/Concentration Recalculation
         ↓
   [Risk Calculation Service] --> Compute New Metrics (using cached prices + positions)
         ↓
   [TimescaleDB] --> Store Risk Snapshot (historical record)
         ↓
   [Redis Cache] --> Update Current Risk Metrics Cache
         ↓
   [WebSocket Server] --> Push Updated Metrics to Connected Clients
         ↓
   [React Frontend] --> Update Dashboard Display
```

**Market Data Update Flow:**

```
[Bloomberg/Market Data Feed] --> Real-time Price Tick
      ↓
[Market Data Service] --> Receive Price Update via WebSocket/FIX
      ↓
[Kafka Topic: market-data-ticks] --> Publish Price Event
      ↓
[Spark Structured Streaming] --> Consume, Aggregate (if needed)
      ↓
[Redis Cache] --> Update Price Cache (key: security_id, value: latest price + timestamp)
      ↓
[Risk Calculation Service] --> Auto-trigger Recalculation if Material Price Change (>1% move in large position)
      ↓
[WebSocket Server] --> Push Updated Metrics to Clients
```

**Historical Data Load Flow (Daily Batch):**

```
[Market Data Vendor] --> End-of-Day Price File (CSV/XML)
      ↓
[ETL Job (Apache Spark Batch)] --> Extract, Transform, Load
      ↓
[PostgreSQL Historical Prices Table] --> Bulk Insert Adjusted Prices
      ↓
[Risk Calculation Service] --> Pre-compute VaR for all portfolios (morning batch before market open)
      ↓
[Redis Cache] --> Warm cache with pre-computed metrics
```

#### 3.1.3 Component Specifications

**Component: Risk Calculation Engine**

| Attribute | Details |
|-----------|---------|
| **Technology** | Java 17 with Spring Boot 3.x OR Scala 2.13 with Apache Spark |
| **Deployment** | Kubernetes pods, horizontally scalable (3+ instances for HA) |
| **Inputs** | Current positions (from cache), current prices (from cache), historical prices (from database) |
| **Outputs** | Calculated risk metrics (VaR, concentrations, leverage) |
| **Performance Target** | Complete VaR calculation for 500-position portfolio in <500ms |
| **Scalability** | Stateless design, can horizontally scale to handle 50+ portfolios concurrently |

**Key Algorithms Implemented:**
- VaR Historical Simulation
- Sector/Issuer aggregation
- Beta/Correlation calculation (daily batch)
- Leverage ratio calculation

**Optimization Strategies:**
- Pre-load historical return matrix into memory at startup
- Parallel VaR calculation using Java parallel streams or Spark RDDs
- Incremental recalculation (only recompute if positions changed >0.1%)
- Result caching with smart invalidation

---

**Component: Real-Time Data Ingestion Pipeline**

| Attribute | Details |
|-----------|---------|
| **Technology** | Apache Spark Structured Streaming 4.0+ |
| **Kafka Topics Consumed** | `position-updates`, `market-data-ticks`, `reference-data-updates` |
| **Processing Mode** | Micro-batch (1-second trigger interval) or Continuous (for ultra-low latency) |
| **Checkpointing** | Enabled (for fault tolerance and exactly-once processing) |
| **Deployment** | Spark cluster on Kubernetes with 5+ executors |

**Processing Logic:**
```scala
// Pseudo-code for position update stream processing
val positionUpdatesStream = spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "kafka:9092")
  .option("subscribe", "position-updates")
  .load()
  .selectExpr("CAST(value AS STRING) as json")
  .select(from_json($"json", positionSchema).as("data"))
  .select("data.*")

positionUpdatesStream
  .writeStream
  .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
    // Write to Redis cache
    batchDF.write
      .format("org.apache.spark.sql.redis")
      .option("table", "current_positions")
      .option("key.column", "position_id")
      .mode("append")
      .save()
    
    // Trigger risk recalculation
    val portfolioIds = batchDF.select("portfolio_id").distinct().collect()
    portfolioIds.foreach { row =>
      riskCalculationService.triggerRecalculation(row.getString(0))
    }
  }
  .start()
```

---

**Component: Web Dashboard (Frontend)**

| Attribute | Details |
|-----------|---------|
| **Technology** | React 18+ with TypeScript, Vite build tool |
| **State Management** | Redux Toolkit (or Zustand for simpler state) |
| **Charting Library** | Recharts (React-friendly) or D3.js (for custom visualizations) |
| **Real-time Communication** | WebSocket (Socket.io or native WebSocket API) |
| **Styling** | Tailwind CSS + shadcn/ui component library |
| **Deployment** | Static files served via Nginx, deployed to CDN |

**Key Features:**
- Auto-reconnecting WebSocket for real-time updates
- Optimistic UI updates (show latest data immediately, reconcile with server if discrepancy)
- Responsive layout (desktop primary, tablet secondary, phone tertiary)
- Dark mode support (optional, nice-to-have)
- Accessibility: ARIA labels, keyboard navigation, screen reader support

**Page Load Performance Targets:**
- Time to Interactive (TTI): <3 seconds on fast 3G connection
- Largest Contentful Paint (LCP): <2.5 seconds
- First Input Delay (FID): <100ms
- Cumulative Layout Shift (CLS): <0.1

### 3.2 Performance Requirements

| Performance Metric | Target | Measurement Method | Priority |
|--------------------|--------|-------------------|----------|
| **Dashboard Refresh Latency (p95)** | <1000ms | Time from position update event in Kafka to display update in browser | CRITICAL |
| **Dashboard Refresh Latency (p99)** | <2000ms | Same as above, 99th percentile | HIGH |
| **VaR Calculation Time** | <500ms | Server-side time to compute VaR for 500-position portfolio | CRITICAL |
| **Page Load Time (TTI)** | <3000ms | Time to Interactive on 4G network | HIGH |
| **API Response Time (p95)** | <200ms | REST API endpoints (e.g., GET /portfolios/{id}/metrics) | HIGH |
| **WebSocket Message Latency** | <100ms | Time from server publish to client receipt | MEDIUM |
| **Concurrent Users Supported** | 100+ | Number of simultaneous users without performance degradation | HIGH |
| **Throughput - Position Updates** | 1000 updates/second | Position update events processed by Spark Streaming | MEDIUM |
| **Throughput - Price Ticks** | 10,000 ticks/second | Market data price updates processed | MEDIUM |
| **Database Query Time (p95)** | <100ms | Historical price queries for VaR calculation | HIGH |
| **Cache Hit Rate** | >95% | Percentage of price/position lookups served from Redis cache | HIGH |

**Performance Testing Approach:**
- **Load Testing**: Use JMeter or Gatling to simulate 100+ concurrent users accessing dashboard
- **Stress Testing**: Gradually increase load to identify breaking point (target: 150+ users before degradation)
- **Spike Testing**: Simulate sudden traffic spike (e.g., 50 to 150 users in 10 seconds) to test auto-scaling
- **Endurance Testing**: Run at 80% of max load for 8 hours (full trading day) to identify memory leaks or degradation
- **Latency Testing**: Measure end-to-end latency from simulated position update to dashboard display using distributed tracing (Jaeger)

### 3.3 Scalability Requirements

**Horizontal Scalability:**

| Component | Scaling Strategy | Initial Deployment | Max Scale |
|-----------|------------------|-------------------|-----------|
| **Risk Calculation Service** | Stateless pods, Kubernetes HPA based on CPU/memory | 3 pods | 20 pods (auto-scale) |
| **API Gateway** | Stateless, load-balanced | 2 instances | 10 instances |
| **WebSocket Server** | Sticky sessions (session affinity), horizontal scaling | 2 instances | 8 instances |
| **Spark Streaming** | Add executors, increase parallelism | 5 executors | 20 executors |
| **Redis Cache** | Redis Cluster with sharding | 3-node cluster | 6-node cluster |
| **PostgreSQL** | Read replicas for query load distribution | 1 primary + 2 replicas | 1 primary + 5 replicas |
| **TimescaleDB** | Time-based partitioning, horizontal sharding (if needed) | Single instance | Multi-node cluster |

**Data Volume Scalability:**

| Data Type | Current Volume | 1-Year Projection | Scaling Approach |
|-----------|----------------|-------------------|------------------|
| **Portfolios** | 50 portfolios | 100 portfolios | Straightforward (no architectural change needed) |
| **Positions per Portfolio** | 200 avg, 500 max | 300 avg, 750 max | Optimize VaR calculation (parallel processing), increase cache size |
| **Unique Securities** | 1,500 securities | 3,000 securities | Increase historical price database size, optimize indexing |
| **Market Data Ticks/Day** | 5M ticks/day | 15M ticks/day | Increase Kafka partition count, add Spark executors |
| **Risk Metric Snapshots** | 50K snapshots/day | 150K snapshots/day | TimescaleDB auto-partitioning handles growth |
| **Historical Price Data** | 1.5M price records/day | 3M price records/day | Database growth ~1GB/month, manageable with compression |

**Geographic Scalability (Future):**
- Phase 1: Single data center (US East)
- Phase 2: Multi-region deployment for global portfolios (EU, APAC)
  - Regional caches for low-latency local access
  - Central risk calculation in primary region
  - Data replication via Kafka MirrorMaker or cloud-native replication

### 3.4 Data Processing Specifications

#### 3.4.1 VaR Calculation Algorithm (Detailed)

**Methodology:** Historical Simulation (non-parametric)

**Mathematical Formulation:**

Given:
- Current portfolio positions: $w_i$ = weight of position $i$, where $\sum w_i = 1$
- Historical return matrix: $R_{i,t}$ = return of position $i$ on historical day $t$, for $t = 1$ to $T$ (where $T = 252$ trading days)
- Confidence level: $\alpha = 0.95$ (95% confidence)

**Step-by-Step Calculation:**

**Step 1: Retrieve Current Portfolio Weights**
```
FOR each position i in portfolio:
  market_value_i = quantity_i * current_price_i
  
portfolio_nav = SUM(market_value_i) for all positions

FOR each position i:
  weight_i = market_value_i / portfolio_nav
```

**Step 2: Retrieve Historical Returns**
```
FOR each position i in portfolio:
  FOR each historical day t from 1 to 252:
    price_today = historical_price(security_i, date_t)
    price_yesterday = historical_price(security_i, date_t - 1 trading day)
    
    IF price_yesterday > 0 THEN
      log_return_i_t = LN(price_today / price_yesterday)
    ELSE
      log_return_i_t = 0  // Handle missing data
```

**Step 3: Compute Portfolio Returns for Each Scenario**
```
FOR each historical day t from 1 to 252:
  portfolio_return_t = 0
  
  FOR each position i:
    portfolio_return_t += weight_i * log_return_i_t
  
  scenario_returns[t] = portfolio_return_t
```

**Step 4: Determine VaR from Return Distribution**
```
sorted_returns = SORT(scenario_returns) from worst (most negative) to best

// 95% confidence means 5% worst scenarios
percentile_index = CEIL(T * (1 - alpha))  // For 252 days, 95% CI: CEIL(252 * 0.05) = 13

var_return_percentile = sorted_returns[percentile_index]  // 13th worst return

var_dollar = -1 * portfolio_nav * var_return_percentile  // Convert to positive dollar loss
var_percent = -1 * var_return_percentile * 100  // Convert to percentage
```

**Step 5: Validation and Output**
```
IF var_dollar < 0 OR var_dollar > portfolio_nav THEN
  LOG_ERROR("VaR calculation error: invalid result", var_dollar, portfolio_nav)
  RETURN error_state

RETURN {
  "var_usd": var_dollar,
  "var_percent": var_percent,
  "confidence_level": 0.95,
  "time_horizon_days": 1,
  "methodology": "Historical Simulation",
  "lookback_days": 252,
  "calculation_timestamp": current_timestamp,
  "num_positions": count(positions),
  "data_quality": {
    "positions_with_full_history": count(positions with 252 days data),
    "positions_excluded": count(positions with <30 days data)
  }
}
```

**Pseudocode Implementation (Java-style):**

```java
public VaRResult calculateHistoricalVaR(Portfolio portfolio, int lookbackDays, double confidenceLevel) {
    // Step 1: Get current positions and weights
    List<Position> positions = portfolio.getPositions();
    double portfolioNav = positions.stream()
        .mapToDouble(p -> p.getQuantity() * p.getCurrentPrice())
        .sum();
    
    Map<Position, Double> weights = positions.stream()
        .collect(Collectors.toMap(
            p -> p,
            p -> (p.getQuantity() * p.getCurrentPrice()) / portfolioNav
        ));
    
    // Step 2: Retrieve historical returns (parallelized for performance)
    Map<Position, List<Double>> historicalReturns = positions.parallelStream()
        .collect(Collectors.toMap(
            p -> p,
            p -> calculateLogReturns(p.getSecurityId(), lookbackDays)
        ));
    
    // Step 3: Compute portfolio returns for each scenario
    List<Double> scenarioReturns = new ArrayList<>(lookbackDays);
    for (int day = 0; day < lookbackDays; day++) {
        double portfolioReturn = 0.0;
        for (Position position : positions) {
            double weight = weights.get(position);
            double positionReturn = historicalReturns.get(position).get(day);
            portfolioReturn += weight * positionReturn;
        }
        scenarioReturns.add(portfolioReturn);
    }
    
    // Step 4: Sort and find percentile
    Collections.sort(scenarioReturns);  // Sort from worst to best
    int percentileIndex = (int) Math.ceil(lookbackDays * (1 - confidenceLevel)) - 1;
    double varReturnPercentile = scenarioReturns.get(percentileIndex);
    
    // Step 5: Convert to dollar VaR
    double varDollar = -1 * portfolioNav * varReturnPercentile;
    double varPercent = -1 * varReturnPercentile * 100;
    
    // Validation
    if (varDollar < 0 || varDollar > portfolioNav) {
        throw new VaRCalculationException("Invalid VaR result: " + varDollar);
    }
    
    return VaRResult.builder()
        .varUsd(varDollar)
        .varPercent(varPercent)
        .confidenceLevel(confidenceLevel)
        .timeHorizonDays(1)
        .lookbackDays(lookbackDays)
        .methodology("Historical Simulation")
        .calculationTimestamp(Instant.now())
        .numPositions(positions.size())
        .build();
}

private List<Double> calculateLogReturns(String securityId, int lookbackDays) {
    List<DailyPrice> prices = historicalPriceRepository.findBySecurityIdAndDateRange(
        securityId,
        LocalDate.now().minusDays(lookbackDays + 1),
        LocalDate.now()
    );
    
    List<Double> returns = new ArrayList<>();
    for (int i = 1; i < prices.size(); i++) {
        double priceToday = prices.get(i).getClosePrice();
        double priceYesterday = prices.get(i - 1).getClosePrice();
        double logReturn = Math.log(priceToday / priceYesterday);
        returns.add(logReturn);
    }
    
    return returns;
}
```

#### 3.4.2 Concentration Calculation Algorithm

**Sector Concentration:**

```python
def calculate_sector_concentration(positions, reference_data):
    """
    Calculate sector concentration for a portfolio
    
    Args:
        positions: List of Position objects with security_id, market_value
        reference_data: Dictionary mapping security_id to sector classification
    
    Returns:
        List of SectorConcentration objects
    """
    sector_aggregation = defaultdict(lambda: {
        'market_value': 0.0,
        'positions': []
    })
    
    portfolio_nav = sum(p.market_value for p in positions)
    
    # Aggregate by sector
    for position in positions:
        sector = reference_data.get(position.security_id, {}).get('gics_sector', 'Unclassified')
        sector_aggregation[sector]['market_value'] += position.market_value
        sector_aggregation[sector]['positions'].append(position)
    
    # Calculate percentages and apply thresholds
    concentration_results = []
    for sector, data in sector_aggregation.items():
        pct_of_portfolio = (data['market_value'] / portfolio_nav) * 100
        
        # Get sector-specific limit from portfolio policy
        sector_limit = get_sector_limit(portfolio_id, sector)  # e.g., 25.0%
        
        # Determine status based on thresholds
        if pct_of_portfolio > sector_limit:
            status = 'RED'
        elif pct_of_portfolio > (sector_limit * 0.80):
            status = 'YELLOW'
        else:
            status = 'GREEN'
        
        concentration_results.append(SectorConcentration(
            sector_name=sector,
            market_value_usd=data['market_value'],
            pct_of_portfolio=pct_of_portfolio,
            limit_pct=sector_limit,
            num_positions=len(data['positions']),
            status=status
        ))
    
    # Sort by market value descending
    concentration_results.sort(key=lambda x: x.market_value_usd, reverse=True)
    
    return concentration_results
```

**Issuer Concentration:**

```python
def calculate_issuer_concentration(positions, reference_data):
    """
    Calculate issuer concentration, aggregating by parent company
    """
    issuer_aggregation = defaultdict(lambda: {
        'market_value': 0.0,
        'positions': []
    })
    
    portfolio_nav = sum(p.market_value for p in positions)
    
    # Aggregate by parent company (issuer)
    for position in positions:
        parent_company_id = reference_data.get(position.security_id, {}).get('parent_company_id')
        if not parent_company_id:
            parent_company_id = position.security_id  # Use security itself if no parent
        
        issuer_aggregation[parent_company_id]['market_value'] += position.market_value
        issuer_aggregation[parent_company_id]['positions'].append(position)
    
    # Calculate percentages
    issuer_results = []
    for issuer_id, data in issuer_aggregation.items():
        pct_of_portfolio = (data['market_value'] / portfolio_nav) * 100
        
        issuer_name = reference_data.get(issuer_id, {}).get('parent_company_name', 'Unknown')
        
        # Apply single-issuer limit (e.g., 5% per issuer for diversified funds)
        single_issuer_limit = 5.0  # Configurable
        
        if pct_of_portfolio > single_issuer_limit:
            status = 'RED'
        elif pct_of_portfolio > (single_issuer_limit * 0.90):
            status = 'YELLOW'
        else:
            status = 'GREEN'
        
        issuer_results.append(IssuerConcentration(
            issuer_id=issuer_id,
            issuer_name=issuer_name,
            market_value_usd=data['market_value'],
            pct_of_portfolio=pct_of_portfolio,
            num_positions=len(data['positions']),
            status=status
        ))
    
    # Sort by percentage descending
    issuer_results.sort(key=lambda x: x.pct_of_portfolio, reverse=True)
    
    # Return top 20 issuers
    return issuer_results[:20]
```

#### 3.4.3 Leverage Calculation Algorithm

```python
def calculate_leverage_ratios(positions, portfolio_nav):
    """
    Calculate gross leverage and net leverage
    
    Args:
        positions: List of Position objects with market_value (positive for long, negative for short)
        portfolio_nav: Portfolio Net Asset Value (USD)
    
    Returns:
        LeverageMetrics object
    """
    total_long_mv = sum(p.market_value for p in positions if p.market_value > 0)
    total_short_mv = sum(abs(p.market_value) for p in positions if p.market_value < 0)
    cash_position = portfolio_nav - (total_long_mv - total_short_mv)
    
    # Gross Leverage = (Long + |Short|) / NAV
    gross_leverage = (total_long_mv + total_short_mv) / portfolio_nav if portfolio_nav > 0 else 0
    
    # Net Leverage = (Long - |Short|) / NAV
    net_leverage = (total_long_mv - total_short_mv) / portfolio_nav if portfolio_nav > 0 else 0
    
    # Cash percentage
    cash_pct = (cash_position / portfolio_nav * 100) if portfolio_nav > 0 else 0
    
    # Apply thresholds
    gross_limit = get_portfolio_limit(portfolio_id, 'gross_leverage')  # e.g., 1.5
    net_limit_upper = get_portfolio_limit(portfolio_id, 'net_leverage_upper')  # e.g., 1.05
    net_limit_lower = get_portfolio_limit(portfolio_id, 'net_leverage_lower')  # e.g., 0.95
    cash_minimum = get_portfolio_limit(portfolio_id, 'cash_minimum_pct')  # e.g., 5.0%
    
    # Determine status
    gross_status = 'RED' if gross_leverage > gross_limit else ('YELLOW' if gross_leverage > gross_limit * 0.90 else 'GREEN')
    net_status = 'RED' if (net_leverage > net_limit_upper or net_leverage < net_limit_lower) else 'GREEN'
    cash_status = 'RED' if cash_pct < cash_minimum else 'GREEN'
    
    return LeverageMetrics(
        gross_leverage=round(gross_leverage, 2),
        net_leverage=round(net_leverage, 2),
        cash_position_usd=round(cash_position, 2),
        cash_position_pct=round(cash_pct, 2),
        total_long_mv=round(total_long_mv, 2),
        total_short_mv=round(total_short_mv, 2),
        gross_leverage_limit=gross_limit,
        net_leverage_limit_range=(net_limit_lower, net_limit_upper),
        cash_minimum_pct=cash_minimum,
        gross_status=gross_status,
        net_status=net_status,
        cash_status=cash_status
    )
```

### 3.5 Integration Requirements

#### Integration 1: Position Management System (EPMS)

**Integration Type:** Real-time API (REST + WebSocket)

**Purpose:** Retrieve current portfolio positions and receive real-time updates when trades are executed

**Technical Details:**

| Aspect | Specification |
|--------|---------------|
| **Protocol** | HTTPS REST API (polling/query) + WebSocket (streaming updates) |
| **Authentication** | OAuth 2.0 Client Credentials flow + API Key in header |
| **Data Format** | JSON |
| **Endpoints** | `GET /api/v1/portfolios/{id}/positions`, `WebSocket wss://epms.company.com/stream` |
| **Request Frequency** | REST: On-demand only (not polling). WebSocket: Continuous connection with heartbeat every 30s |
| **Expected Response Time** | <500ms for REST calls, <100ms for WebSocket message delivery |
| **Error Handling** | Retry with exponential backoff (3 retries). If WebSocket disconnects, reconnect automatically with backoff. Display stale data warning if connection lost >60s |
| **Data Validation** | Validate JSON schema, check for negative NAV, ensure position market values sum correctly |

**Integration Flow:**
```
1. Initial Load (Page Load):
   Dashboard --> GET /api/v1/portfolios/{id}/positions
   EPMS --> Returns current positions snapshot
   Dashboard --> Cache positions, display metrics

2. Real-Time Updates:
   Dashboard --> Establish WebSocket connection wss://epms.company.com/stream
   Dashboard --> Send subscription message: {"action": "subscribe", "portfolio_ids": ["portfolio_123"]}
   EPMS --> Send position update events when trades occur
   Dashboard --> Receive event, update cache, recalculate metrics, display update

3. Error Handling:
   If WebSocket connection drops:
     - Attempt reconnect with exponential backoff (1s, 2s, 4s, 8s, max 30s)
     - Display "Reconnecting..." status
     - Fall back to REST polling every 10s if WebSocket unavailable for >2 minutes
```

**Security:**
- TLS 1.3 for all connections
- OAuth token refreshed every 3600 seconds (1 hour)
- API key rotated quarterly
- IP whitelist: Risk dashboard backend IPs allowed

---

#### Integration 2: Market Data Provider (Bloomberg/IEX Cloud)

**Integration Type:** Real-time streaming data feed

**Purpose:** Receive real-time price updates for all securities in active portfolios

**Technical Details:**

| Aspect | Specification |
|--------|---------------|
| **Protocol** | Bloomberg BPIPE (proprietary) OR IEX Cloud WebSocket |
| **Authentication** | Bloomberg: Terminal subscription. IEX: API token in connection string |
| **Data Format** | Binary (Bloomberg) or JSON (IEX Cloud) |
| **Subscription Model** | Dynamic: Subscribe to tickers when positions added, unsubscribe when removed from all portfolios |
| **Expected Latency** | <500ms from exchange timestamp to receipt (p95) |
| **Throttling** | Bloomberg: Unlimited for subscribed securities. IEX: 100 messages/second per connection |
| **Error Handling** | If feed disconnects: Display stale price warnings, continue using last known prices, attempt reconnect every 10s |
| **Fallback Strategy** | If primary feed unavailable >5 minutes, switch to backup provider (e.g., Polygon.io) if configured |

**Data Processing:**
```
Market Data Feed --> Streaming price tick
         ↓
Market Data Service --> Parse message, extract symbol + price + timestamp
         ↓
Publish to Kafka topic: "market-data-ticks"
         ↓
Spark Streaming consumes topic
         ↓
Update Redis cache: SET security:{symbol}:price = {price, timestamp}
         ↓
Check if price change is material (>1% for positions >2% of any portfolio)
         ↓
If material: Trigger risk recalculation for affected portfolios
         ↓
Push updated metrics to dashboard clients
```

---

#### Integration 3: Reference Data System (ERDM)

**Integration Type:** Batch load (daily) + REST API (on-demand lookups)

**Purpose:** Retrieve security master data, sector classifications, issuer mappings

**Technical Details:**

| Aspect | Specification |
|--------|---------------|
| **Protocol** | HTTPS REST API |
| **Authentication** | API Key |
| **Data Format** | JSON |
| **Endpoints** | `GET /api/v1/securities/{id}`, `GET /api/v1/securities?ids=comma_separated_list` |
| **Batch Load Schedule** | Daily at 2:00 AM ET (loads updated reference data for all securities) |
| **Request Frequency** | On-demand lookups only for securities not in cache |
| **Cache Strategy** | Redis cache with 24-hour TTL, refreshed by daily batch load |
| **Error Handling** | If API unavailable, use cached data (potentially stale). Flag securities without reference data as "Unclassified" |

**Batch Load Process:**
```
Daily 2:00 AM ET:
ETL Job (Spark) --> Query /api/v1/securities?universe=all_active
ERDM --> Returns full reference data dump (JSON)
ETL Job --> Parse, transform, validate
ETL Job --> Bulk load into PostgreSQL reference_data table
ETL Job --> Refresh Redis cache with updated mappings
```

---

#### Integration 4: Corporate SSO / Identity Provider

**Integration Type:** SAML 2.0 Single Sign-On

**Purpose:** Authenticate users and retrieve role/group memberships for authorization

**Technical Details:**

| Aspect | Specification |
|--------|---------------|
| **Protocol** | SAML 2.0 |
| **Identity Provider** | Microsoft Entra ID (formerly Azure AD) or Okta |
| **Service Provider** | Risk Dashboard (SP-initiated SSO flow) |
| **Assertion Format** | XML with user attributes: email, name, groups[] |
| **Session Duration** | 12 hours absolute, 60 minutes idle timeout |
| **Certificate Management** | SAML signing certificates rotated annually, dashboard stores IdP public cert |
| **Logout** | Single Logout (SLO) supported - logout from dashboard logs out of corporate SSO |

**SAML Flow:**
```
1. User navigates to https://risk-dashboard.company.com
2. Dashboard checks for valid session cookie
3. No valid session --> Redirect to SSO IdP login page with SAML request
4. User enters credentials + MFA
5. IdP validates credentials
6. IdP generates SAML assertion (signed XML with user attributes)
7. IdP redirects back to dashboard with SAML response
8. Dashboard validates signature, extracts user attributes
9. Dashboard creates session, grants access based on user role/groups
10. Dashboard displays portfolio selector screen
```

---

## Section 4: Non-Functional Requirements

### 4.1 Security Requirements

#### Authentication
- **Requirement**: All users must authenticate via corporate Single Sign-On (SAML 2.0) before accessing any portfolio data
- **Implementation**: SAML integration with Microsoft Entra ID or Okta
- **Password Policy**: Enforced by corporate IdP (minimum 12 characters, complexity requirements, MFA required)
- **Session Management**: 60-minute idle timeout, 12-hour absolute timeout, secure httpOnly cookies

#### Authorization
- **Requirement**: Role-Based Access Control (RBAC) with least-privilege principle
- **Roles**: Portfolio Manager, Risk Officer, Trader, Compliance Officer, Read-Only Viewer
- **Portfolio-Level Access**: Users can only access portfolios explicitly granted to them or their AD groups
- **Data Filtering**: Backend enforces authorization (no client-side only filtering)

#### Data Encryption
- **In Transit**: TLS 1.3 for all connections (web browser to server, server to server)
- **At Rest**: AES-256 encryption for database storage (managed by database encryption features)
- **Key Management**: Encryption keys stored in Hardware Security Module (HSM) or cloud KMS (AWS KMS, Azure Key Vault)

#### Audit Logging
- **Requirement**: All user actions and data access logged to tamper-proof audit log
- **Log Contents**: Timestamp, user ID, action (LOGIN, VIEW_PORTFOLIO, EXPORT_DATA, ACKNOWLEDGE_ALERT, CONFIGURE_THRESHOLD), portfolio ID, IP address, session ID
- **Log Storage**: Append-only storage (WORM - Write Once Read Many), retained for 7 years per regulatory requirements
- **Log Monitoring**: Automated monitoring for suspicious activity (failed login attempts, unusual access patterns)

#### Vulnerability Management
- **Requirement**: Regular security assessments and patching
- **Activities**: Quarterly vulnerability scans, annual penetration testing, monthly dependency updates (patch critical CVEs within 7 days)
- **Secure Development**: OWASP Top 10 awareness, code reviews for security, SAST/DAST in CI/CD pipeline

### 4.2 Reliability & Availability

#### Availability Target
- **SLA**: 99.5% uptime during market hours (9:30 AM - 4:00 PM ET, Monday-Friday)
- **Allowed Downtime**: ~1.5 hours per month, ~19 minutes per trading day (on average)
- **Maintenance Windows**: Scheduled maintenance outside market hours (weekends, before 7:00 AM or after 6:00 PM ET)

#### Fault Tolerance
- **Component Redundancy**: All critical services deployed with multiple replicas (minimum 3 for stateless services)
- **Database HA**: PostgreSQL with synchronous replication (primary + 2 replicas), automatic failover
- **Cache HA**: Redis Cluster with replication factor 2 (every shard has 1 replica)
- **Load Balancing**: Health-checked load balancers distribute traffic across healthy instances only

#### Disaster Recovery
- **RPO (Recovery Point Objective)**: 5 minutes - Maximum acceptable data loss
- **RTO (Recovery Time Objective)**: 15 minutes - Maximum acceptable downtime for full recovery
- **Backup Strategy**: 
  - Real-time replication to secondary data center (hot standby)
  - Daily full database backups to object storage (retained for 90 days)
  - Hourly incremental backups during market hours
- **DR Testing**: Quarterly disaster recovery drills (failover to secondary data center)

#### Graceful Degradation
- **Behavior When Data Feeds Fail**: Display last known values with prominent "STALE DATA" warning and timestamp
- **Behavior When Calculation Service Overloaded**: Queue recalculation requests, show "Calculating..." indicator, serve cached values until calculation completes
- **Behavior When Database Unavailable**: Serve data from cache (Redis) for up to 10 minutes while attempting reconnection

### 4.3 Compliance Requirements

#### Regulatory Frameworks
- **SOX (Sarbanes-Oxley)**: Internal controls over financial data, audit trails, segregation of duties
- **SEC Rule 17a-4**: Electronic records retention (audit logs, risk reports retained for 7 years in non-rewriteable format)
- **SEC Rule 15c3-1**: Net capital requirements for broker-dealers (concentration limits, leverage limits)
- **FINRA**: If applicable to broker-dealer operations, recordkeeping and supervision requirements

#### Data Retention
- **Audit Logs**: 7 years in immutable storage
- **Risk Metric Snapshots**: 2 years of intraday data (30-second granularity), 7 years of end-of-day snapshots
- **Portfolio Positions**: 7 years of daily position snapshots
- **User Access Logs**: 7 years

#### Compliance Reporting
- **Daily Risk Breach Report**: Automated report listing all portfolio limit breaches, delivered to Risk Committee by 7:00 AM following trading day
- **Monthly Access Review**: Report of all user access grants/revocations, reviewed by Compliance Officer
- **Quarterly Security Assessment**: Vulnerability scan report and remediation status
- **Annual SOX Controls Testing**: Audit of system controls for financial data integrity

### 4.4 Monitoring & Observability

#### Application Monitoring

**Metrics to Track:**
- **Request latency**: p50, p95, p99 for all API endpoints
- **Throughput**: Requests per second, position updates per second, price ticks per second
- **Error rates**: HTTP 4xx, 5xx error rates by endpoint
- **Cache hit/miss rates**: Redis cache performance
- **VaR calculation time**: Histogram of calculation durations
- **WebSocket connection count**: Number of active real-time connections

**Alerting Thresholds:**
- Critical: p95 latency >2 seconds, error rate >5%, cache hit rate <80%
- Warning: p95 latency >1 second, error rate >2%, cache hit rate <90%

**Tools:** Prometheus (metrics collection), Grafana (dashboards), PagerDuty (alerting)

#### Infrastructure Monitoring

**Metrics to Track:**
- **CPU utilization**: Per pod/instance
- **Memory utilization**: Per pod/instance, watch for memory leaks
- **Disk I/O**: Database server disk latency and throughput
- **Network bandwidth**: Kafka topic throughput, WebSocket traffic
- **Pod health**: Kubernetes pod restarts, crash loop backoffs
- **Database connections**: Active connections, connection pool saturation

**Alerting:**
- Critical: CPU >90% sustained for 5 minutes, memory >95%, disk >90% full, pod crash loops
- Warning: CPU >75%, memory >80%, disk >80%

**Tools:** Kubernetes metrics-server, Prometheus node exporter, Cloud provider monitoring (CloudWatch, Azure Monitor)

#### Distributed Tracing

**Purpose:** Track end-to-end latency from position update to dashboard display

**Implementation:** OpenTelemetry instrumentation, Jaeger for trace storage and visualization

**Trace Spans:**
1. Position update received by Kafka
2. Spark Streaming processes event
3. Redis cache updated
4. Risk calculation triggered
5. VaR computed
6. Result cached
7. WebSocket message sent to clients
8. Browser renders update

**Sample Trace Queries:**
- "Show me all requests with >2 second latency"
- "Trace the path of position update event ABC123"
- "Which service is the bottleneck in VaR calculation flow?"

#### Business Metrics Dashboard

**Metrics for Business Stakeholders:**
- **User Adoption**: Daily active users, portfolios monitored per day
- **Alert Statistics**: Alerts generated per day, alert acknowledgment rate, alert resolution time
- **System Health**: Uptime percentage, average dashboard refresh latency
- **Data Quality**: Percentage of positions with complete reference data, price feed uptime

**Audience:** Product managers, Risk Officers, CTO

**Tool:** Grafana dashboard with business-friendly visualizations

---

## Section 5: Testing Requirements

### 5.1 Unit Testing

**Scope:** Test individual functions and methods in isolation

**Test Coverage Target:** >80% code coverage for business logic, >95% for risk calculation algorithms

**Key Test Scenarios:**

**VaR Calculation:**
- [ ] Test VaR calculation with known dataset, verify result matches manual calculation
- [ ] Test with portfolio of 1 position (edge case)
- [ ] Test with portfolio of 500 positions (max size)
- [ ] Test with missing historical data (some positions have <252 days)
- [ ] Test with zero-price scenario (handle division by zero)
- [ ] Test with negative returns (portfolio loss scenarios)
- [ ] Verify 95th percentile selection is correct (13th worst out of 252)

**Concentration Analysis:**
- [ ] Test sector aggregation with positions from multiple sectors
- [ ] Test issuer aggregation with parent company mapping
- [ ] Test with "Unclassified" sector (positions without sector mapping)
- [ ] Test threshold logic (green/yellow/red status)
- [ ] Test edge case: portfolio with 100% in one sector
- [ ] Verify percentages sum to 100%

**Leverage Calculation:**
- [ ] Test long-only portfolio (no short positions)
- [ ] Test long/short portfolio with both long and short positions
- [ ] Test edge case: zero NAV (handle gracefully)
- [ ] Test edge case: negative NAV (error condition)
- [ ] Test cash calculation accuracy
- [ ] Verify gross and net leverage formulas

**Framework:** JUnit 5 (Java), pytest (Python), Jest (TypeScript/JavaScript)

### 5.2 Integration Testing

**Scope:** Test interactions between components and external systems

**Key Test Scenarios:**

**Position Management System Integration:**
- [ ] Test REST API call to retrieve positions, verify response parsing
- [ ] Test WebSocket connection establishment and authentication
- [ ] Test receiving position update event, verify data flows to cache
- [ ] Test WebSocket reconnection after simulated disconnect
- [ ] Test OAuth token refresh flow
- [ ] Test error handling when EPMS returns HTTP 500

**Market Data Provider Integration:**
- [ ] Test subscribing to price feeds for list of tickers
- [ ] Test receiving price tick, verify parsing and caching
- [ ] Test unsubscribing from ticker when position removed
- [ ] Test handling of delayed or out-of-order price ticks
- [ ] Test fallback to backup provider if primary feed unavailable

**Database Integration:**
- [ ] Test querying historical prices for VaR calculation
- [ ] Test inserting risk metric snapshot to TimescaleDB
- [ ] Test Redis cache set/get operations
- [ ] Test database connection pool under load (100 concurrent queries)
- [ ] Test failover to read replica if primary database unavailable

**Kafka Event Streaming:**
- [ ] Test publishing position update to Kafka topic
- [ ] Test Spark Streaming consuming from Kafka topic
- [ ] Test exactly-once processing semantics (no duplicate events)
- [ ] Test handling of Kafka broker failure (consumer rebalancing)

**Framework:** Testcontainers (for spinning up database/Kafka in Docker), WireMock (for mocking external HTTP APIs)

### 5.3 Performance Testing

**Scope:** Validate system meets latency and throughput targets under load

**Test Scenarios:**

**Load Test 1: Steady-State Dashboard Usage**
- **Objective**: Verify system handles 100 concurrent users during market hours
- **Setup**: 100 virtual users, each accessing 1-2 portfolios, keeping dashboard open for 4 hours
- **Expected Behavior**: 
  - p95 dashboard refresh latency <1 second
  - No errors or timeouts
  - CPU and memory utilization stable (no memory leaks)
- **Tool**: Gatling or Apache JMeter

**Load Test 2: VaR Calculation Performance**
- **Objective**: Verify VaR calculation completes in <500ms for 500-position portfolio
- **Setup**: Benchmark VaR calculation for portfolios of varying sizes (50, 100, 250, 500 positions)
- **Expected Results**:
  - 50 positions: <100ms
  - 100 positions: <150ms
  - 250 positions: <300ms
  - 500 positions: <500ms
- **Tool**: JMH (Java Microbenchmark Harness) or Python timeit

**Stress Test: Peak Market Volatility**
- **Objective**: Verify system handles high volume of price updates and position changes
- **Setup**: Simulate market volatility event with 10,000 price ticks/second + 500 position updates/second
- **Expected Behavior**:
  - All events processed without loss
  - Dashboard refresh latency may increase to p95 <2 seconds (acceptable during extreme volatility)
  - System recovers to normal performance within 5 minutes after load subsides

**Endurance Test: Full Trading Day**
- **Objective**: Verify system stability over full 6.5-hour trading day
- **Setup**: Run at 80% of max load (80 concurrent users) for 6.5 hours
- **Expected Behavior**:
  - No memory leaks (memory utilization stable)
  - No performance degradation over time
  - Error rate remains <0.1%

### 5.4 Security Testing

**Scope:** Validate security controls and identify vulnerabilities

**Test Scenarios:**

**Authentication Testing:**
- [ ] Test unauthorized access attempt (no session cookie) - verify redirect to SSO
- [ ] Test expired session - verify automatic logout and redirect
- [ ] Test SAML assertion tampering - verify signature validation rejects tampered assertion
- [ ] Test brute-force login attempts - verify account lockout after 5 failed attempts

**Authorization Testing:**
- [ ] Test user accessing unauthorized portfolio - verify HTTP 403 Forbidden response
- [ ] Test privilege escalation attempt (modify JWT token to grant admin role) - verify rejection
- [ ] Test horizontal privilege escalation (User A accessing User B's portfolio) - verify blocked
- [ ] Test RBAC enforcement - verify each role has correct permissions

**Vulnerability Scanning:**
- [ ] Run OWASP ZAP automated scan against web application
- [ ] Perform SQL injection testing on API parameters
- [ ] Test for Cross-Site Scripting (XSS) vulnerabilities
- [ ] Test for Cross-Site Request Forgery (CSRF) protection
- [ ] Test for sensitive data exposure (error messages, logs, API responses)
- [ ] Test for insecure deserialization
- [ ] Test for XML External Entity (XXE) attacks in SAML processing

**Penetration Testing:**
- [ ] Engage third-party security firm for annual penetration test
- [ ] Simulate attacks: credential stuffing, session hijacking, MITM
- [ ] Test network segmentation and firewall rules
- [ ] Test for privilege escalation paths

**Tools:** OWASP ZAP, Burp Suite, Nessus, third-party penetration testing firm

### 5.5 User Acceptance Testing (UAT)

**Scope:** Validate system meets business requirements with real users

**Participants:** 5-10 Portfolio Managers, 2-3 Risk Officers, 2 Traders, 1 Compliance Officer

**UAT Scenarios:**

**Scenario 1: Morning Risk Review**
- **User**: Portfolio Manager
- **Task**: Log in, select portfolio, review overnight risk metrics, identify any threshold breaches
- **Acceptance Criteria**:
  - [ ] User can log in within 30 seconds
  - [ ] Dashboard loads portfolio data within 3 seconds
  - [ ] All key metrics clearly visible (VaR, leverage, cash, concentrations)
  - [ ] User can interpret metrics without additional training
  - [ ] Any alerts are prominently displayed

**Scenario 2: Monitor Real-Time Impact of Trade**
- **User**: Portfolio Manager
- **Task**: Execute trade in trading system, observe real-time update in risk dashboard within 1 second
- **Acceptance Criteria**:
  - [ ] Dashboard refreshes automatically (no manual refresh needed)
  - [ ] Updated metrics reflect new position
  - [ ] VaR changes by expected amount (roughly proportional to position size)
  - [ ] Concentration metrics update correctly

**Scenario 3: Investigate Risk Threshold Breach**
- **User**: Risk Officer
- **Task**: Receive alert that portfolio exceeded limit, investigate root cause, document action plan
- **Acceptance Criteria**:
  - [ ] Alert clearly indicates which portfolio and which metric breached
  - [ ] User can drill down to see contributing positions
  - [ ] User can view historical trend to see when breach occurred
  - [ ] User can document notes (if note-taking feature provided)

**Scenario 4: Monitor Multiple Portfolios**
- **User**: Risk Officer
- **Task**: Monitor 10 portfolios simultaneously, identify which portfolios are approaching limits
- **Acceptance Criteria**:
  - [ ] User can see high-level status of multiple portfolios (ideally a portfolio list view)
  - [ ] Color coding (green/yellow/red) makes it easy to spot problem portfolios
  - [ ] User can quickly navigate between portfolio detail views

**UAT Sign-Off Criteria:**
- [ ] 90% of UAT participants rate system as "meets expectations" or "exceeds expectations"
- [ ] All critical defects resolved
- [ ] <5 medium-severity defects accepted for post-launch fix
- [ ] No high or critical usability issues remaining

---

## Section 6: Implementation Plan

### 6.1 Phased Delivery Approach

#### Phase 1: MVP (Months 1-4)

**Scope:**
- Core VaR calculation engine (Historical Simulation, 1-day, 95%)
- Sector concentration analysis
- Top positions display
- Cash and leverage monitoring
- Basic web dashboard (single portfolio view)
- SSO authentication and RBAC
- Integration with Position Management System (REST API)
- Integration with market data provider (WebSocket streaming)
- Basic threshold alerting

**Deliverables:**
- Working dashboard accessible to pilot user group (10 users)
- Supports 5 pilot portfolios (up to 300 positions each)
- Deployed to UAT environment

**Success Criteria:**
- [ ] VaR calculation accuracy validated against manual calculations (<0.1% difference)
- [ ] Dashboard refresh latency <1 second for pilot portfolios
- [ ] 95% uptime during pilot phase
- [ ] Positive feedback from pilot users (>80% satisfaction)

**Dependencies:**
- API access to Position Management System (must be granted by IT)
- Market data feed subscription activated (procurement must complete vendor contract)
- UAT infrastructure provisioned (cloud resources or on-prem servers)

**Risks:**
- Delay in API access could push timeline by 2-4 weeks
- Market data vendor contract negotiations could delay by 4-8 weeks (mitigation: use free tier of backup provider for pilot)

---

#### Phase 2: Enhanced Metrics and Scalability (Months 5-6)

**Scope:**
- Geographic concentration analysis
- Beta and correlation calculations
- Issuer concentration (parent company aggregation)
- Drill-down from portfolio to position-level risk contribution
- Performance optimization for 500-position portfolios
- Scale to 50 portfolios
- Horizontal scaling (Kubernetes auto-scaling)
- Historical trending (30-day intraday history, 1-year daily history)

**Deliverables:**
- Enhanced dashboard with additional metrics
- Production deployment (rollout to 50 portfolios, 50 users)
- Performance testing report showing <1 second latency for 500-position portfolios

**Success Criteria:**
- [ ] All additional metrics calculated accurately
- [ ] System supports 50 concurrent users with <1 second latency
- [ ] 99.5% uptime during production launch
- [ ] User adoption rate >80% (50+ users actively using system daily)

---

#### Phase 3: Advanced Features and Integration (Months 7-9)

**Scope:**
- Multi-portfolio comparison view for Risk Officers
- Customizable dashboard layouts and preferences
- Export functionality (PDF risk reports, Excel data export)
- Integration with downstream compliance monitoring system
- Advanced alerting (email notifications, escalation rules)
- Mobile-responsive design enhancements
- Historical scenario analysis (e.g., "What was VaR during March 2020 COVID crash?")
- API for programmatic access (for quants and advanced users)

**Deliverables:**
- Full-featured production system
- Rollout to 100 portfolios, 100+ users
- Integration with compliance platform complete

**Success Criteria:**
- [ ] All advanced features delivered and tested
- [ ] Integration with compliance system passes UAT
- [ ] User satisfaction survey >85% positive
- [ ] System reliably handles 100+ concurrent users

---

#### Phase 4: Optimization and Expansion (Months 10-12+)

**Scope:**
- Parametric VaR and Monte Carlo VaR methodologies (user-selectable)
- Predictive analytics and scenario simulation ("what-if" analysis)
- Machine learning-based anomaly detection
- Multi-region deployment for global users (EU, APAC data centers)
- Advanced visualization (3D risk surfaces, interactive correlation matrices)
- Mobile native apps (iOS, Android)

**Deliverables:**
- Advanced analytics platform
- Global deployment (multi-region)
- Mobile apps published to app stores

**Success Criteria:**
- [ ] Advanced features adoption by power users (>30% of users)
- [ ] Multi-region deployment reduces latency for international users by >50%
- [ ] Mobile app adoption >40% of user base

### 6.2 Resource Requirements

**Development Team:**

| Role | Phase 1 (Months 1-4) | Phase 2 (Months 5-6) | Phase 3 (Months 7-9) | Phase 4 (Months 10-12) |
|------|----------------------|----------------------|----------------------|------------------------|
| **Backend Engineers (Java/Scala)** | 3 FTE | 3 FTE | 2 FTE | 2 FTE |
| **Frontend Engineers (React/TypeScript)** | 2 FTE | 2 FTE | 2 FTE | 2 FTE |
| **Data Engineers (Spark/Kafka)** | 2 FTE | 1 FTE | 1 FTE | 1 FTE |
| **DevOps/SRE Engineers** | 1 FTE | 1 FTE | 1 FTE | 1 FTE |
| **QA Engineers** | 1 FTE | 2 FTE | 2 FTE | 1 FTE |
| **UX Designer** | 1 FTE | 0.5 FTE | 0.5 FTE | 0.5 FTE |
| **Product Manager** | 1 FTE | 1 FTE | 1 FTE | 1 FTE |
| **Solution Architect** | 1 FTE (50% time) | 0.5 FTE | 0.5 FTE | 0.5 FTE |
| **Security Engineer** | 0.5 FTE | 0.5 FTE | 0.5 FTE | 0.5 FTE |
| **Data Scientist (for Phase 4 ML)** | - | - | - | 2 FTE |

**Infrastructure:**

- **Cloud Infrastructure** (AWS/Azure):
  - Kubernetes cluster (5-10 nodes initially, auto-scaling to 20 nodes)
  - Managed PostgreSQL database (primary + 2 replicas)
  - Managed Kafka cluster (3 brokers)
  - Redis cluster (3 nodes with replication)
  - TimescaleDB instance
  - Object storage for backups
  - Load balancers, NAT gateways, VPN
  - **Estimated Cost**: $12K-$15K/month (Phase 1), $25K-$30K/month (Phase 3 production)

- **Third-Party Services**:
  - Market data subscription (Bloomberg or IEX Cloud): $5K-$50K/month depending on tier
  - Monitoring tools (Datadog/New Relic): $2K/month
  - SSO/Identity provider (if separate subscription): $500/month

**Total Budget Estimate:**

- **Phase 1 (MVP, 4 months)**: $750K development + $60K infrastructure + $100K third-party = $910K
- **Phase 2-3 (8 months)**: $1.2M development + $240K infrastructure + $200K third-party = $1.64M
- **Phase 4 (6 months)**: $800K development + $180K infrastructure + $150K third-party = $1.13M
- **Total (18 months)**: $3.66M

### 6.3 Dependencies and Prerequisites

**Critical Path Dependencies:**

1. **Position Management System API Access** (Must have by Month 1, Week 2)
   - Owner: IT Integration Team
   - Action: Provision service account, grant API access, provide documentation
   - Risk: High - Blocks all development if delayed
   - Mitigation: Start API access request process immediately (lead time 4-6 weeks)

2. **Market Data Subscription** (Must have by Month 1, Week 3)
   - Owner: Procurement + Market Data Vendor
   - Action: Execute contract, activate subscription, configure data feeds
   - Risk: High - No real-time pricing without this
   - Mitigation: Use free tier of backup provider (IEX Cloud) for initial development if primary vendor delayed

3. **Cloud Infrastructure Provisioning** (Must have by Month 1, Week 1)
   - Owner: DevOps Team
   - Action: Set up Kubernetes cluster, databases, Kafka, networking
   - Risk: Medium - Can use development environment temporarily
   - Mitigation: Provision dev environment immediately, production environment by Month 3

4. **Historical Price Data Load** (Must have by Month 2, Week 1)
   - Owner: Data Engineering Team
   - Action: ETL job to load 252+ days of historical prices from data warehouse
   - Risk: Medium - Needed for VaR calculation development
   - Mitigation: Use sample dataset for initial development

5. **Reference Data Integration** (Must have by Month 2, Week 2)
   - Owner: Data Engineering Team
   - Action: Integration with ERDM for sector classifications, issuer mappings
   - Risk: Medium - Needed for concentration analysis
   - Mitigation: Use static reference data file for initial development

6. **SSO Integration** (Must have by Month 3, Week 1)
   - Owner: Security Team + SSO Vendor
   - Action: Configure SAML integration, test authentication flow
   - Risk: Medium - Can use basic auth for initial development
   - Mitigation: Start SAML configuration early, use temporary basic auth for UAT if needed

**Organizational Prerequisites:**

- [ ] Project sponsor identified and committed (Chief Risk Officer or Chief Technology Officer)
- [ ] Budget approved ($3.66M total, $910K for Phase 1)
- [ ] Development team hired or allocated (11 FTE for Phase 1)
- [ ] Risk Management team designates product owner and SMEs (2-3 people, 20% time)
- [ ] Pilot user group identified (10 portfolio managers + 2 risk officers)
- [ ] IT Security team completes security architecture review and approves design
- [ ] Procurement completes vendor selection and contracting for market data provider

### 6.4 Risks and Mitigation Strategies

| Risk | Likelihood | Impact | Mitigation Strategy |
|------|------------|--------|---------------------|
| **Delay in Position Management System API access** | Medium | High | Start access request process in Month 0 (pre-kickoff). Develop against mock API initially. Escalate to CTO if blocked. |
| **Market data vendor contract delays** | Medium | High | Use free tier of backup provider (IEX Cloud) for development and pilot. Prioritize vendor contract in procurement queue. |
| **VaR calculation performance does not meet <500ms target** | Medium | High | Prototype VaR algorithm early (Month 1). Conduct performance benchmarking by Month 2. Optimize or simplify methodology if needed. Consider in-memory compute (Redis + Spark) or GPU acceleration. |
| **Historical price data quality issues** | Medium | Medium | Conduct data quality assessment in Month 1. Identify gaps and cleansing requirements. Build data validation into ETL pipeline. |
| **User adoption lower than expected** | Medium | Medium | Involve pilot users early in design (Month 1). Conduct UX testing frequently. Provide training and onboarding materials. Collect feedback and iterate. |
| **Security vulnerability discovered** | Low | High | Follow secure development practices (SAST/DAST in CI/CD). Conduct quarterly vulnerability scans. Engage penetration testing firm in Month 5 (before production launch). |
| **Database performance issues at scale** | Medium | Medium | Design for scalability from start (horizontal sharding, read replicas). Load test early and often. Monitor database performance in UAT. |
| **Key team member leaves project** | Low | High | Cross-train team members. Document architecture and design decisions. Maintain knowledge base. |
| **Scope creep** | High | Medium | Strict change control process. Product Owner has authority to accept/defer feature requests. Maintain backlog for post-MVP enhancements. |
| **Integration complexity underestimated** | Medium | Medium | Allocate buffer time for integration work (20% contingency). Conduct integration spike tests early. Engage integration partners early for collaboration. |

---

## Section 7: Success Metrics and KPIs

### 7.1 Technical Performance Metrics

| Metric | Target | Measurement Frequency | Responsible Party |
|--------|--------|----------------------|-------------------|
| **Dashboard Refresh Latency (p95)** | <1000ms | Real-time monitoring | DevOps Team |
| **Dashboard Refresh Latency (p99)** | <2000ms | Real-time monitoring | DevOps Team |
| **VaR Calculation Time (p95)** | <500ms | Real-time monitoring | Backend Team |
| **System Uptime (Market Hours)** | >99.5% | Daily | SRE Team |
| **API Error Rate** | <0.5% | Real-time monitoring | Backend Team |
| **WebSocket Connection Stability** | <1% disconnections | Real-time monitoring | Backend Team |
| **Cache Hit Rate** | >95% | Real-time monitoring | Backend Team |
| **Concurrent Users Supported** | 100+ | Quarterly load test | QA Team |

### 7.2 Business Impact Metrics

| Metric | Baseline | Target (6 months post-launch) | Measurement Method |
|--------|----------|-------------------------------|-------------------|
| **Time to Risk Visibility** | 30-60 minutes (batch) | <1 second (real-time) | Timestamp analysis (trade to dashboard update) |
| **Risk Limit Breaches (Proactive Detection Rate)** | 40% detected intraday | 95% detected intraday | Audit log analysis |
| **Portfolio Manager Productivity** | Baseline = 100% | 115% (15% improvement) | Survey + time tracking |
| **Manual Risk Calculation Requests** | 50-80 per day | <10 per day | Help desk ticket volume |
| **Time to Investigate Risk Breach** | 2 hours average | <5 minutes average | Timestamp analysis (alert to resolution) |
| **Risk-Adjusted Returns (Sharpe Ratio)** | Portfolio-specific baseline | +8-12% improvement | Performance attribution analysis |

### 7.3 User Adoption Metrics

| Metric | Target | Measurement Method |
|--------|--------|-------------------|
| **Daily Active Users (DAU)** | 90% of licensed users | Analytics platform (Google Analytics or similar) |
| **Average Session Duration** | >30 minutes | Analytics platform |
| **Portfolios Monitored Per User Per Day** | >2 portfolios | Application logs |
| **Feature Utilization Rate** | >70% users use core features weekly | Feature usage analytics |
| **User Satisfaction Score** | >85% satisfied or very satisfied | Quarterly user survey (NPS or CSAT) |
| **Support Ticket Volume** | <5 tickets per 100 users per month | Help desk system |

### 7.4 Data Quality Metrics

| Metric | Target | Measurement Method |
|--------|--------|-------------------|
| **Position Data Completeness** | >99.5% of portfolios have complete position data | Data validation checks |
| **Market Data Uptime** | >99.9% during market hours | Feed monitoring |
| **Reference Data Coverage** | >99% of securities have sector classification | Reference data quality reports |
| **VaR Calculation Success Rate** | >99% of calculations complete without errors | Application logs |
| **Data Freshness (Positions)** | >95% of updates within 5 seconds | Timestamp analysis |
| **Data Freshness (Prices)** | >95% of updates within 15 seconds | Timestamp analysis |

---

## Section 8: Assumptions and Constraints

### 8.1 Assumptions

**Technical Assumptions:**
1. Position Management System API provides data with <5 second latency from trade execution
2. Market data provider delivers real-time prices with <500ms latency (p95)
3. Corporate network bandwidth is sufficient for WebSocket streaming to 100+ concurrent users
4. Database storage can accommodate 2 years of intraday position snapshots (~5TB estimated)
5. Historical price data is available and accurate for all securities for at least 252 trading days
6. Reference data system provides sector classifications for >99% of securities

**Organizational Assumptions:**
7. Risk Management team will dedicate product owner (50% time) and SMEs (20% time each) for project duration
8. Pilot users will dedicate 5-10 hours total for UAT feedback
9. IT Security team will complete security review within 4 weeks of request
10. Procurement can execute market data vendor contract within 8 weeks
11. Budget and staffing allocations remain stable for 18-month project duration

**Business Assumptions:**
12. Portfolio managers will adopt real-time risk monitoring as part of daily workflow
13. Real-time risk visibility will lead to better decision-making and improved risk-adjusted returns
14. Regulatory requirements (SOX, SEC Rule 17a-4) will not change significantly during project
15. User base will grow from 50 users (Phase 1) to 100+ users (Phase 3) over 18 months

### 8.2 Constraints

**Technical Constraints:**
1. **Technology Stack**: Must use firm's approved technology stack (Java/Scala backend, React frontend, PostgreSQL/Redis data stores)
2. **Network Access**: Production systems cannot access public internet directly (all external API calls must route through proxy)
3. **Database Storage Limit**: Phase 1 limited to 10TB total storage (accommodates ~2 years historical data)
4. **Integration Constraints**: Must integrate with existing Position Management System without requiring vendor modifications
5. **Browser Support**: Must support Chrome, Firefox, Safari, Edge (latest 2 versions). IE11 not required.

**Business Constraints:**
6. **Budget**: Phase 1 budget capped at $910K. Phase 2-3 budget requires separate approval.
7. **Timeline**: Phase 1 MVP must launch within 6 months to meet business urgency
8. **Implementation Disruption**: Cannot disrupt existing end-of-day risk reporting during implementation
9. **Regulatory Compliance**: Must comply with SOX, SEC Rule 17a-4, and internal data security policies (non-negotiable)
10. **Approval Requirements**: Requires sign-off from Chief Risk Officer and Chief Information Officer before production deployment

**Organizational Constraints:**
11. **Staffing**: Development team size limited to 12 FTE maximum (resource availability)
12. **Access to Subject Matter Experts**: Risk analysts available for requirements clarification max 4 hours/week
13. **Change Control**: All production changes require CAB (Change Advisory Board) approval with 5-day lead time
14. **Testing Windows**: Production-like performance testing can only occur during non-market hours (before 9:00 AM or after 6:00 PM ET)

**Data Constraints:**
15. **Historical Data**: VaR lookback limited to 252 trading days due to data warehouse retention policy
16. **Market Data Licensing**: Market data subscription limits number of simultaneous users based on license tier
17. **PII Restrictions**: User email addresses and personal information must not be stored in application database (use SSO attributes only)

---

## Section 9: Appendices

### 9.1 Glossary of Terms

| Term | Definition |
|------|------------|
| **Value at Risk (VaR)** | Statistical measure of the potential loss in portfolio value over a specified time horizon at a given confidence level. Example: 1-day 95% VaR of $5M means there is a 5% probability the portfolio will lose more than $5M in a single day. |
| **Historical Simulation** | Non-parametric VaR methodology that uses actual historical return scenarios to estimate future risk. Assumes historical return patterns are representative of future possibilities. |
| **Confidence Level** | Probability threshold used in VaR calculation. 95% confidence means VaR represents the loss threshold exceeded only 5% of the time (5% worst scenarios). |
| **Gross Leverage** | Ratio of total long market value plus absolute value of short positions to portfolio NAV. Measures total capital deployed including both long and short exposures. Formula: (Long MV + |Short MV|) / NAV. |
| **Net Leverage** | Ratio of net exposure (long minus short) to portfolio NAV. Measures directional market exposure. Formula: (Long MV - |Short MV|) / NAV. |
| **Sector Concentration** | Percentage of portfolio invested in a particular industry sector (e.g., Technology, Healthcare, Financials). High concentration increases sector-specific risk. |
| **Beta** | Measure of portfolio volatility relative to a market benchmark. Beta > 1 means portfolio is more volatile than market. Beta = 1 means portfolio moves in line with market. |
| **Correlation** | Statistical measure of how closely two return series move together. Ranges from -1 (perfect inverse relationship) to +1 (perfect positive relationship). |
| **Position Management System (PMS)** | Enterprise system that tracks current portfolio holdings, quantities, cost basis, and market values. Source of truth for position data. |
| **NAV (Net Asset Value)** | Total value of portfolio assets minus liabilities. Represents the equity value of the portfolio. |
| **SAML (Security Assertion Markup Language)** | XML-based open standard for Single Sign-On (SSO) authentication. Allows users to authenticate once and access multiple applications. |
| **Role-Based Access Control (RBAC)** | Authorization model where permissions are assigned to roles (Portfolio Manager, Risk Officer, etc.) and users are assigned to roles. |
| **Logarithmic Return (Log Return)** | Return calculated as ln(P_t / P_{t-1}). Logarithmic returns are time-additive and symmetric for gains and losses, preferred for VaR calculations. |
| **Lookback Period** | Historical time window used for VaR calculation. Typical lookback is 252 trading days (approximately 1 year). |
| **GICS Sector** | Global Industry Classification Standard. 11 high-level sector classifications: Energy, Materials, Industrials, Consumer Discretionary, Consumer Staples, Health Care, Financials, Information Technology, Communication Services, Utilities, Real Estate. |
| **Issuer** | Entity that issues a security (e.g., Apple Inc. is the issuer of AAPL stock). Concentration analysis aggregates by issuer to measure single-entity risk. |

### 9.2 Reference Documents

1. **Enterprise User Story Template** - Standard template for documenting feature requirements (internal document)
2. **Apache Spark Structured Streaming Programming Guide** - Documentation for real-time data processing framework
3. **Apache Spark ML Statistics Documentation** - Reference for statistical functions (correlation, summarization) in Spark MLlib
4. **Portfolio Risk Management Best Practices** - Industry guidelines for portfolio risk measurement and monitoring
5. **SEC Rule 17a-4** - Electronic records retention requirements for financial institutions
6. **SOX Compliance Requirements** - Sarbanes-Oxley internal controls and audit requirements
7. **OWASP Top 10** - Web application security risks and mitigation strategies
8. **SAML 2.0 Technical Overview** - Specification for Single Sign-On authentication
9. **GICS Methodology** - Global Industry Classification Standard documentation from MSCI and S&P
10. **VaR Calculation Methodologies** - Technical papers on Historical Simulation, Parametric, and Monte Carlo VaR methods

### 9.3 Stakeholder Contact List

| Stakeholder Name | Role | Responsibility | Contact Information |
|------------------|------|----------------|---------------------|
| **Jennifer Williams** | Chief Risk Officer (CRO) | Executive Sponsor, Final Approver | jennifer.williams@company.com |
| **David Chen** | VP Portfolio Management | Business Sponsor, User Community Lead | david.chen@company.com |
| **Michael Rodriguez** | Risk Management Officer | Product Owner, Requirements Definition | michael.rodriguez@company.com |
| **Sarah Chen** | Senior Portfolio Manager | Pilot User, UAT Lead | sarah.chen@company.com |
| **James Patterson** | Head Trader | Stakeholder, Workflow Integration | james.patterson@company.com |
| **Lisa Thompson** | Chief Technology Officer (CTO) | Technical Sponsor, Infrastructure Approval | lisa.thompson@company.com |
| **Robert Kim** | Solution Architect | Technical Lead, Architecture Design | robert.kim@company.com |
| **Emily Martinez** | Product Manager | Product Owner (Tech Side), Roadmap | emily.martinez@company.com |
| **Amit Patel** | Lead Backend Engineer | Development Lead, VaR Calculation Engine | amit.patel@company.com |
| **Jessica Lee** | Lead Frontend Engineer | Development Lead, Dashboard UI | jessica.lee@company.com |
| **Carlos Rodriguez** | DevOps Lead | Infrastructure, Deployment, Monitoring | carlos.rodriguez@company.com |
| **Priya Sharma** | QA Manager | Test Strategy, UAT Coordination | priya.sharma@company.com |
| **Mark Johnson** | Information Security Officer | Security Review, Compliance Approval | mark.johnson@company.com |
| **Angela Davis** | Compliance Officer | Regulatory Requirements, Audit Logging | angela.davis@company.com |

### 9.4 Related User Stories and Epics

**Parent Epic:**
- **SPARK-FINTECH-000**: Financial Services Analytics Platform - Comprehensive initiative to build enterprise-grade financial analytics capabilities using Apache Spark

**Related User Stories (Future):**
- **SPARK-FINTECH-002**: Options Greeks and Derivatives Risk Analytics - Extend risk dashboard to support options portfolios with Greeks (delta, gamma, vega, theta) calculations
- **SPARK-FINTECH-003**: Fixed Income Risk Metrics - Add duration, convexity, spread risk, and credit risk metrics for bond portfolios
- **SPARK-FINTECH-004**: Multi-Currency Portfolio Consolidation - Support portfolios with positions in multiple currencies with FX risk analysis
- **SPARK-FINTECH-005**: Stress Testing and Scenario Analysis - "What-if" analysis tool for simulating portfolio performance under hypothetical market scenarios
- **SPARK-FINTECH-006**: Regulatory Reporting Automation - Automated generation of regulatory risk reports (SEC Form PF, Form ADV)
- **SPARK-FINTECH-007**: Algorithmic Trading Integration - Pre-trade risk checks integrated with order management system
- **SPARK-FINTECH-008**: Machine Learning Risk Prediction - ML models to predict risk regime changes and portfolio vulnerabilities

### 9.5 Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2024-01-15 | Emily Martinez (Product Manager) | Initial creation of comprehensive user story based on business requirements |

---

**Document Status:** DRAFT - Pending Review  
**Next Review Date:** 2024-01-22  
**Approvers Required:** Michael Rodriguez (Risk Product Owner), Robert Kim (Solution Architect), Mark Johnson (InfoSec), Lisa Thompson (CTO)

---

**END OF USER STORY DOCUMENT**

