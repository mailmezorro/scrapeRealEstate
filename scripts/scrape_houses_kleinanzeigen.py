from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
import json
import logging
import time
import os
import psycopg2
from psycopg2 import sql
from selenium.common.exceptions import TimeoutException
import re
from datetime import datetime


def scrape_attributes(logger, driver, attributes): 
    results = {}
    
    try:
        wait = WebDriverWait(driver, 10)
        wait.until(EC.presence_of_element_located((By.XPATH, "//*[@id='viewad-details']")))
        
        for attribute in attributes:
            try:
                element = driver.find_element(By.XPATH, f"//*[@id='viewad-details']//li[contains(text(), '{attribute}')]").text
                value = element.split('\n')[-1].strip()
                match = re.search(r'^([\d\.]+)', value)

                if match:
                    results[attribute] = match.group(1).replace('.', '')
                else:
                    results[attribute] = value
                
                logger.info(f"{attribute} gefunden: {element} auf {driver.current_url}")
            except NoSuchElementException:
                logger.warning(f"{attribute} not found at {driver.current_url}")
                results[attribute] = None 
    except Exception as e:
        logger.error(f"Error retrieving from {driver.current_url}: {e.__class__.__name__}", exc_info=True)

    return results


def wait_for_element(driver, xpath, timeout=10, poll_frequency=0.1):
    end_time = time.time() + timeout
    while time.time() < end_time:
        try:
            element = driver.find_element(By.XPATH, xpath)
            if element.is_displayed():  
                return element
        except NoSuchElementException:
            time.sleep(poll_frequency)  
    raise TimeoutException(f"Element with XPath '{xpath}' not found within {timeout} seconds")


def scrape_header(logger,driver):
    """Scrape the title, price, location, creation date and view counter of an ad."""
    results = {}

    wait = WebDriverWait(driver, 10)
    wait.until(EC.presence_of_element_located((By.XPATH, "//*[@id='viewad-cntr-num']")))
    
    # title
    try:
        title = driver.find_element(By.XPATH, "//*[@id='viewad-main-info']//*[@id='viewad-title']").text
        logger.info(f"title found: {title} auf {driver.current_url}")
        results['title'] = title  
        
    except NoSuchElementException:
        logger.warning(f"title not found at {driver.current_url}")
        results['title'] = None  
        
    # price
    try:
        price = driver.find_element(By.XPATH, "//*[@id='viewad-main-info']//*[@id='viewad-price']").text
        value = price.split('\n')[-1].strip()
        match = re.search(r'([\d.]+)', value)

        if match:
            try:
                results['price'] = float(match.group(1).replace('.', ''))
            except ValueError:
                results['price'] = match.group(1)
        else:
            try:
                results['price'] = float(value.replace('.', ''))
            except ValueError:
                results['price'] = 0.0

        
    except NoSuchElementException:
        logger.warning(f"price not found at  {driver.current_url}")
        results['price'] = None    

    # location
    try:
        location = driver.find_element(By.XPATH, "//*[@id='viewad-main-info']//*[@id='viewad-locality']").text
        logger.info(f"location found: {location} auf {driver.current_url}")
        results['location'] = location
        
    except NoSuchElementException:
        logger.warning(f"location not found at  {driver.current_url}")
        results['location'] = None   

    # creation date
    try:
        creation_date = driver.find_element(By.XPATH, "//*[@id='viewad-extra-info']//span[1]").text
        logger.info(f"creation_date found: {creation_date} auf {driver.current_url}")
        results['creation_date'] = datetime.strptime(creation_date, "%d.%m.%Y")
        
    except NoSuchElementException:
        logger.warning(f"creation_date not found at  {driver.current_url}")
        results['creation_date'] = None  

    # view counter
    try:
        view_counter = wait_for_element(driver, "//*[@id='viewad-cntr-num']").text
        logger.info(f"view_counter found: {view_counter} auf {driver.current_url}")
        results['view_counter'] = int(view_counter)
        
    except NoSuchElementException:
        logger.warning(f"view_counter not found at  {driver.current_url}")
        results['view_counter'] = None      

    return results


def scrape_description(logger,driver):
    result = {}
    try:
        wait = WebDriverWait(driver, 10)
        wait.until(EC.presence_of_element_located((By.XPATH, "//*[@id='viewad-description']")))
        
        element = driver.find_element(By.XPATH, f"//*[@id='viewad-description']").text
        logger.info(f"Description found at {driver.current_url}")
        result["description"] = element
    except NoSuchElementException:
        logger.warning(f"Description not found at {driver.current_url}")
        result["description"] = None
    except Exception as e:
        logger.error(f"Error retrieving von {driver.current_url}: {e.__class__.__name__}", exc_info=True)

    return result



def scrape_right_sidebar(logger,driver):
    """Scrape the company, author, number_of_ads, id"""
    result = {}
    
    # Company
    try:
        wait = WebDriverWait(driver, 10)
        wait.until(EC.presence_of_element_located((By.XPATH, "//*[@id='viewad-contact']")))
        element = driver.find_element(By.CSS_SELECTOR, ".userprofile-vip").text
        logger.info(f"Company found at {driver.current_url}")
        result["Company"] = element
    except NoSuchElementException:
        logger.warning(f"Company not found at {driver.current_url}")
        result["Company"] = None
    except Exception as e:
        logger.error(f"Error retrieving von {driver.current_url}: {e.__class__.__name__}", exc_info=True)
        
        
    # author    
    try:
        element = driver.find_element(By.XPATH, f"//*[@id='viewad-contact']").text
        logger.info(f"Author found at {driver.current_url}")
        result["Author"] = element
    except NoSuchElementException:
        logger.warning(f"Author not found at {driver.current_url}")
        result["Author"] = None
    except Exception as e:
        logger.error(f"Error retrieving von {driver.current_url}: {e.__class__.__name__}", exc_info=True)    


    # number_of_ads    
    try:
        ads_text = driver.find_element(By.ID, 'poster-other-ads-link').text
        logger.info(f"number_of_ads found at {driver.current_url} using ID")
    except NoSuchElementException:
        try:
            ads_text = driver.find_element(By.CSS_SELECTOR, '.bizteaser--numads').text
            logger.info(f"number_of_ads found at {driver.current_url} using CSS_SELECTOR")
        except NoSuchElementException:
            logger.warning(f"number_of_ads not found at {driver.current_url}")
            result["number_of_ads"] = None
            ads_text = None
        except Exception as e:
            logger.error(f"Error retrieving von {driver.current_url}: {e.__class__.__name__}", exc_info=True)  

    if ads_text:
        ads_number = ''.join(filter(str.isdigit, ads_text))
        result["number_of_ads"] = int(ads_number)
    else:
        result["number_of_ads"] = None

    
    # id_ad  
    try:
        element = driver.find_element(By.XPATH, f"//*[@id='viewad-ad-id-box']").text
        element = element.split('\n')[-1]
        logger.info(f"number_of_ads found at {driver.current_url}")
        result["id_ad"] = int(element)
    except NoSuchElementException:
        logger.warning(f"id not found at {driver.current_url}")
        result["id_ad"] = None
    except Exception as e:
        logger.error(f"Error retrieving {driver.current_url}: {e.__class__.__name__}", exc_info=True)

    return result
