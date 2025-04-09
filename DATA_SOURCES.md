# Data Sources

## ANEEL Distributed Generation Ventures (Micro and Mini)

### Primary Dataset
- **Name**: Relação de Empreendimentos de Geração Distribuída  
- **Description**: Official registry of distributed generation ventures in Brazil, including photovoltaic systems.  
- **URL**: [https://dadosabertos.aneel.gov.br/dataset/relacao-de-empreendimentos-de-geracao-distribuida](https://dadosabertos.aneel.gov.br/dataset/relacao-de-empreendimentos-de-geracao-distribuida)  
- **License**: [Creative Commons Attribution 4.0 International (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/)  
- **Update Frequency**: Daily  

### Key Resources Used

1. **Administrative Data**  
   - **Resource**: Relação dos empreendimentos de geração distribuída 
   - **Content**: Venture locations, capacities, owner and substation information  
   - **Direct Download**: [CSV File](https://dadosabertos.aneel.gov.br/datastore/dump/b1bd71e7-d0ad-4214-9053-cbd58e9564a7?bom=True) 

2. **Technical Specifications**  
   - **Resource**: Informações Técnicas - Geração Fotovoltaica  
   - **Content**: PV system technical details (modules, inverters)  
   - **Direct Download**: [CSV File](https://dadosabertos.aneel.gov.br/datastore/dump/49fa9ca0-f609-4ae3-a6f7-b97bd0945a3a?bom=True)

## Usage Notes
1. **Preprocessing**:  
   - This tool directly downloads from the above links in `/data/raw/`.  
   - Column names must remain unchanged for the cleaner to work.

2. **Attribution**:  
   When using this data, cite:  
   > "Brazilian Electricity Regulatory Agency (ANEEL), Distributed Generation Registry [Year]. Available at: [dataset URL]"