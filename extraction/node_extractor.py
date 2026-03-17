"""Node extraction module using GPT and RAG"""
import pandas as pd
import json
import time
from tqdm import tqdm
from langchain.document_loaders.csv_loader import CSVLoader
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain.chains import ConversationalRetrievalChain
from langchain.vectorstores import FAISS
from langchain.callbacks import get_openai_callback
from typing import List, Tuple, Dict


from prompt.prompt_templates import ExtractionPrompts


class NodeExtractor:
    """Extracts nodes using GPT-4 with RAG"""
    
    def __init__(self, openai_api_key: str, rag_csv_path: str = "RAG_foodbeat.csv"):
        """
        Initialize NodeExtractor
        
        Args:
            openai_api_key (str): OpenAI API key
            rag_csv_path (str): Path to RAG CSV file
        """
        self.openai_api_key = openai_api_key
        self.rag_csv_path = rag_csv_path
        self.chain = None
        self._setup_rag()
    
    def _setup_rag(self):
        """Setup RAG (Retrieval-Augmented Generation) system"""
        # Load CSV data
        loader = CSVLoader(
            file_path=self.rag_csv_path, 
            encoding="utf-8", 
            csv_args={'delimiter': ','}
        )
        data = loader.load()
        
        # Create embeddings and vector store
        embeddings = OpenAIEmbeddings()
        vectorstore = FAISS.from_documents(data, embeddings)
        
        # Create conversational chain
        self.chain = ConversationalRetrievalChain.from_llm(
            llm=ChatOpenAI(temperature=0.7, model_name='gpt-4o'),
            retriever=vectorstore.as_retriever()
        )
    
    def _make_prompt(self, input_menu: List[str], input_parent_nodes: List[str]) -> str:
        
        return ExtractionPrompts.get_extraction_prompt(input_menu, input_parent_nodes)

    def _conversational_chat(self, query: str) -> Tuple[str, Dict]:
        """
        Execute chat with GPT
        
        Args:
            query (str): Query to send to GPT
            
        """
        with get_openai_callback() as cb:
            result = self.chain.invoke({"question": query, "chat_history": []})
            
            token_info = {
                'total_tokens': cb.total_tokens,
                'prompt_tokens': cb.prompt_tokens,
                'completion_tokens': cb.completion_tokens,
                'total_cost': cb.total_cost
            }
        
        return result['answer'], token_info
    
    def extract_nodes(self, menu_groups: List[List[str]], parent_nodes: List[str], 
                     output_dir: str, today: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Extract nodes for menu groups using GPT
        
        Args:
            menu_groups (list): List of menu groups
            parent_nodes (list): List of parent node names
            output_dir (str): Output directory for results
            today (str): Today's date string
        """
        import os
        
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        start_time = time.time()
        cnt = 1
        
        result_df = pd.DataFrame()
        token_df = pd.DataFrame()
        
        for items in tqdm(menu_groups, desc="Processing items"):
            # Create prompt
            try:
                prompt = ExtractionPrompts.get_extraction_prompt(items, parent_nodes)
            except Exception as e:
                print(f"{cnt}번째 프롬프트 정의 중 오류 발생: {e}")
                continue
            
            # Call GPT API
            try:
                gpt_output, token_info = self._conversational_chat(prompt)
            except Exception as e:
                print(f"{cnt}번째 GPT API 호출 중 오류 발생: {e}")
                continue
            
            # Parse response
            try:
                data = json.loads(gpt_output.replace("```", '').replace('json', ''))
                
                file_name = f"({cnt}){','.join(items[0:5])},,,"
                with open(f'{output_dir}/{file_name}.json', 'w', encoding='utf-8') as file:
                    json.dump(data, file, ensure_ascii=False, indent=4)
                
                data_df = pd.DataFrame(data['result'])
                
            except Exception as e:
                print(f"{cnt}번째 응답 데이터 처리 중 오류 발생: {e}")
                print("문제의 content:", gpt_output)
                continue
            
            # Save token info
            try:
                token_one = pd.DataFrame(token_info, index=[0])
                
                result_df = pd.concat([result_df, data_df], axis=0)
                token_df = pd.concat([token_df, token_one], axis=0)
                
                # Save intermediate results
                result_df.to_csv(f'{output_dir}/node_extraction_result.csv', encoding='utf-8')
                token_df.to_csv(f'{output_dir}/node_extraction_token_info.csv', encoding='utf-8')
                
            except Exception as e:
                print(f"{cnt}번째 토큰정보, 데이터 병합 및 저장 과정 중 오류 발생: {e}")
                continue
            
            time.sleep(1)
            cnt += 1
        
        end_time = time.time()
        elapsed_minutes = (end_time - start_time) / 60
        
        print(f"{cnt}번 호출 완료, 작업 총 실행 시간: {elapsed_minutes:.2f}분")
        print('$', round(token_df['total_cost'].sum(), 2))
        
        return result_df, token_df
