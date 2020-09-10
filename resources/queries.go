//
// Copyright (c) 2019 LG Electronics Inc.
// SPDX-License-Identifier: Apache-2.0
//

package resources

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"time"
        logger "github.com/sirupsen/logrus"
	"github.com/hyperledger/fabric-chaincode-go/shim"
	pb "github.com/hyperledger/fabric-protos-go/peer"
)

// =========================================================================================
// Business related queries
// =========================================================================================

func GetBusinesses(stub shim.ChaincodeStubInterface) pb.Response {
	queryResults, err := getDocumentsByDocType(stub, BUSINESS_DOCTYPE)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}
	return shim.Success(queryResults)
}

func GetBusinessesWithPagination(stub shim.ChaincodeStubInterface, pageSize int32, bookmark string) pb.Response {
	queryResults, err := getDocumentsByDocTypeWithPagination(stub, BUSINESS_DOCTYPE, pageSize, bookmark)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}
	return shim.Success(queryResults)
}

// =========================================================================================
// DataCategory related queries
// =========================================================================================

func GetDataCategories(stub shim.ChaincodeStubInterface) pb.Response {
	queryResults, err := getDocumentsByDocType(stub, DATA_CATEGORY_DOCTYPE)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}
	return shim.Success(queryResults)
}

func GetDataCategoriesWithPagination(stub shim.ChaincodeStubInterface, pageSize int32, bookmark string) pb.Response {
	queryResults, err := getDocumentsByDocTypeWithPagination(stub, DATA_CATEGORY_DOCTYPE, pageSize, bookmark)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}
	return shim.Success(queryResults)
}

type PopularDataCategory struct {
	DataCategoryId string `json:"dataCategoryId"`
	Number         int32  `json:"number"`
}

func GetPopularDataCategories(stub shim.ChaincodeStubInterface, size int32) pb.Response {
	queryResults, err := getDocumentsByDocType(stub, DATA_CONTRACT_TYPE_DOCTYPE)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}
	dataContractTypeArray := []DataContractType{}
	err = json.Unmarshal(queryResults, &dataContractTypeArray)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}

	if len(dataContractTypeArray) == 0 {
		return shim.Success([]byte("[]"))
	}
	responseMap := make(map[string]int32)
	for i := 0; i < len(dataContractTypeArray); i++ {
		_, ok := responseMap[dataContractTypeArray[i].CategoryID]
		if ok {
			responseMap[dataContractTypeArray[i].CategoryID]++
		} else {
			responseMap[dataContractTypeArray[i].CategoryID] = 1
		}
	}

	var popularDataCategories []PopularDataCategory
	for k, v := range responseMap {
		popularDataCategories = append(popularDataCategories, PopularDataCategory{k, v})
	}

	sort.Slice(popularDataCategories, func(i, j int) bool {
		return popularDataCategories[i].Number > popularDataCategories[j].Number
	})

	var returnedPopularDataCategories []PopularDataCategory
	for i := 0; i < int(size) && i < len(popularDataCategories); i++ {
		returnedPopularDataCategories = append(returnedPopularDataCategories, popularDataCategories[i])
	}

	returnedPopularDataCategoriesAsBytes, err := json.Marshal(returnedPopularDataCategories)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}

	if returnedPopularDataCategories == nil {
		returnedPopularDataCategoriesAsBytes = []byte("[]")
	}

	return shim.Success(returnedPopularDataCategoriesAsBytes)
}

// =========================================================================================
// DataContractType related queries
// =========================================================================================

func GetRecommendedDataContractType(stub shim.ChaincodeStubInterface) pb.Response {
	queryResults, err := getDocumentsByDocType(stub, DATA_CONTRACT_TYPE_DOCTYPE)
	if err != nil {
		return shim.Error(err.Error())
	}
	dataContractTypes := []DataContractType{}
	err = json.Unmarshal(queryResults, &dataContractTypes)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}
	recommendedIndex := rand.Intn(len(dataContractTypes))
	dataContractType := dataContractTypes[recommendedIndex]

	dataContractTypeAsbytes, err := json.Marshal(dataContractType)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}
	return shim.Success(dataContractTypeAsbytes)
}

func GetDataContractTypesAfterTimeStamp(stub shim.ChaincodeStubInterface, timestamp string) pb.Response {
	queryTime, err := time.Parse("2006-01-02T15:04:05.000Z", timestamp)
	if err != nil {
		return shim.Error(err.Error())
	}

	queryResults, err := getDocumentsByDocType(stub, DATA_CONTRACT_TYPE_DOCTYPE)
	if err != nil {
		return shim.Error(err.Error())
	}
	contractTypes := []DataContractType{}
	err = json.Unmarshal(queryResults, &contractTypes)
	if err != nil {
		return shim.Error(err.Error())
	}
	res := []DataContractType{}
	for _, contractType := range contractTypes {
		if contractType.CreationDateTime.After(queryTime) {
			res = append(res, contractType)
		}
	}
	byteRes, err := json.Marshal(res)
	if err != nil {
		return shim.Error(err.Error())
	}
	return shim.Success(byteRes)
}

func GetDataContractTypes(stub shim.ChaincodeStubInterface) pb.Response {
	queryResults, err := getDocumentsByDocType(stub, DATA_CONTRACT_TYPE_DOCTYPE)
	if err != nil {
		return shim.Error(err.Error())
	}
	return shim.Success(queryResults)
}

func GetDataContractTypesWithPagination(stub shim.ChaincodeStubInterface, pageSize int32, bookmark string) pb.Response {
	queryResults, err := getDocumentsByDocTypeWithPagination(stub, DATA_CONTRACT_TYPE_DOCTYPE, pageSize, bookmark)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}
	return shim.Success(queryResults)
}

func GetDataContractTypesByCategory(stub shim.ChaincodeStubInterface, categoryId string) pb.Response {
	queryResults, err := getDocumentsByDocTypeAndCategory(stub, DATA_CONTRACT_TYPE_DOCTYPE, categoryId)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}
	return shim.Success(queryResults)
}

func GetDataContractTypesByCategoryWithPagination(stub shim.ChaincodeStubInterface, categoryId string, pageSize int32, bookmark string) pb.Response {
	queryResults, err := getDocumentsByDocTypeAndCategoryWithPagination(stub, DATA_CONTRACT_TYPE_DOCTYPE, categoryId, pageSize, bookmark)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}
	return shim.Success(queryResults)
}

func getDocumentsByDocTypeAndCategory(stub shim.ChaincodeStubInterface, docType string, categoryId string) ([]byte, error) {
	query := "{\"selector\":{\"docType\":\"" + docType + "\", \"categoryId\":\"" + categoryId + "\"}}"
	return getQueryResultForQueryString(stub, query)
}
func getDocumentsByDocTypeAndCategoryWithPagination(stub shim.ChaincodeStubInterface, docType string, categoryId string, pageSize int32, bookmark string) ([]byte, error) {
	query := "{\"selector\":{\"docType\":\"" + docType + "\", \"categoryId\":\"" + categoryId + "\"}}"
	return getQueryResultForQueryStringWithPagination(stub, query, pageSize, bookmark)
}

func GetDataContractTypesByProvider(stub shim.ChaincodeStubInterface, providerId string) pb.Response {
	queryResults, err := getDocumentsByDocTypeAndProvider(stub, DATA_CONTRACT_TYPE_DOCTYPE, providerId)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}
	return shim.Success(queryResults)
}

func GetDataContractTypesByProviderWithPagination(stub shim.ChaincodeStubInterface, providerId string, pageSize int32, bookmark string) pb.Response {
	queryResults, err := getDocumentsByDocTypeAndProviderWithPagination(stub, DATA_CONTRACT_TYPE_DOCTYPE, providerId, pageSize, bookmark)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}
	return shim.Success(queryResults)
}

type PopularDataContractType struct {
	DataContractType DataContractType `json:"dataContractType"`
	Number           int32            `json:"number"`
}

func GetPopularDataContractTypes(stub shim.ChaincodeStubInterface, size int32) pb.Response {
	queryResults, err := getDocumentsByDocType(stub, DATA_CONTRACT_DOCTYPE)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}
	dataContractArray := []DataContract{}
	err = json.Unmarshal(queryResults, &dataContractArray)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}

	if len(dataContractArray) == 0 {
		return shim.Success([]byte("[]"))
	}
	responseMap := make(map[string]PopularDataContractType)
	for i := 0; i < len(dataContractArray); i++ {
		_, ok := responseMap[dataContractArray[i].DataContractTypeID]
		if ok {
			popularDataContractType := responseMap[dataContractArray[i].DataContractTypeID]
			popularDataContractType.Number++
			responseMap[dataContractArray[i].DataContractTypeID] = popularDataContractType
		} else {
			dataContractType, err := GetDataContractTypeStructState(stub, dataContractArray[i].DataContractTypeID)
			if err == nil {
				popularDataContractType := PopularDataContractType{
					DataContractType: dataContractType,
					Number:           1,
				}
				responseMap[dataContractArray[i].DataContractTypeID] = popularDataContractType
			}
		}
	}

	var popularDataContractTypes []PopularDataContractType
	for _, v := range responseMap {
		popularDataContractTypes = append(popularDataContractTypes, v)
	}

	sort.Slice(popularDataContractTypes, func(i, j int) bool {
		return popularDataContractTypes[i].Number > popularDataContractTypes[j].Number
	})

	var returnedPopularDataContractTypes []PopularDataContractType
	for i := 0; i < int(size) && i < len(popularDataContractTypes); i++ {
		returnedPopularDataContractTypes = append(returnedPopularDataContractTypes, popularDataContractTypes[i])
	}

	returnedPopularDataContractTypesAsBytes, err := json.Marshal(returnedPopularDataContractTypes)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}

	if returnedPopularDataContractTypes == nil {
		returnedPopularDataContractTypesAsBytes = []byte("[]")
	}

	return shim.Success(returnedPopularDataContractTypesAsBytes)
}

// =========================================================================================
// DataContract related queries
// =========================================================================================

func GetDataContracts(stub shim.ChaincodeStubInterface) pb.Response {
	queryResults, err := getDocumentsByDocType(stub, DATA_CONTRACT_DOCTYPE)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}
	return shim.Success(queryResults)
}

func GetDataContractsWithPagination(stub shim.ChaincodeStubInterface, pageSize int32, bookmark string) pb.Response {
	queryResults, err := getDocumentsByDocTypeWithPagination(stub, DATA_CONTRACT_DOCTYPE, pageSize, bookmark)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}
	return shim.Success(queryResults)
}

func GetDataContractsByProvider(stub shim.ChaincodeStubInterface, providerId string) pb.Response {
	queryResults, err := getDocumentsByDocTypeAndProvider(stub, DATA_CONTRACT_DOCTYPE, providerId)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}
	return shim.Success(queryResults)
}

func GetDataContractsByProviderWithPagination(stub shim.ChaincodeStubInterface, providerId string, pageSize int32, bookmark string) pb.Response {
	queryResults, err := getDocumentsByDocTypeAndProviderWithPagination(stub, DATA_CONTRACT_DOCTYPE, providerId, pageSize, bookmark)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}
	return shim.Success(queryResults)
}

func GetDataContractsByConsumer(stub shim.ChaincodeStubInterface, consumerId string) pb.Response {
	queryResults, err := getDocumentsByDocTypeAndConsumer(stub, DATA_CONTRACT_DOCTYPE, consumerId)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}
	return shim.Success(queryResults)
}

func GetDataContractsByConsumerWithPagination(stub shim.ChaincodeStubInterface, consumerId string, pageSize int32, bookmark string) pb.Response {
	queryResults, err := getDocumentsByDocTypeAndConsumerWithPagination(stub, DATA_CONTRACT_DOCTYPE, consumerId, pageSize, bookmark)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}
	return shim.Success(queryResults)
}

func SelectDataSetContractsToUpload(stub shim.ChaincodeStubInterface, dataContractTypeId string) pb.Response {
	queryResults, err := getDocumentsByDocTypeAndDataContractTypeAndFileStatus(stub, DATA_CONTRACT_DOCTYPE, dataContractTypeId, "PROPOSAL")
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}
	return shim.Success(queryResults)
}

func SelectDataSetContractsToUploadWithPagination(stub shim.ChaincodeStubInterface, dataContractTypeId string, pageSize int32, bookmark string) pb.Response {
	queryResults, err := getDocumentsByDocTypeAndDataContractTypeAndFileStatusWithPagination(stub, DATA_CONTRACT_DOCTYPE, dataContractTypeId, "PROPOSAL", pageSize, bookmark)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}
	return shim.Success(queryResults)
}

func SelectBusinessDataSetsToUpload(stub shim.ChaincodeStubInterface, provider string) pb.Response {
	queryResults, err := getDocumentsByDocTypeAndProviderAndFileStatus(stub, DATA_CONTRACT_DOCTYPE, provider, "PROPOSAL")
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}
	return shim.Success(queryResults)
}

func SelectBusinessDataSetsToUploadWithPagination(stub shim.ChaincodeStubInterface, provider string, pageSize int32, bookmark string) pb.Response {
	queryResults, err := getDocumentsByDocTypeAndProviderAndFileStatusWithPagination(stub, DATA_CONTRACT_DOCTYPE, provider, "PROPOSAL", pageSize, bookmark)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}
	return shim.Success(queryResults)
}

func SelectDataContractsByDataContractType(stub shim.ChaincodeStubInterface, dataContractTypeID string) pb.Response {
	queryResults, err := getDocumentsByDocTypeAndContractType(stub, DATA_CONTRACT_DOCTYPE, dataContractTypeID)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}
	return shim.Success(queryResults)
}

func SelectDataContractsByDataContractTypeWithPagination(stub shim.ChaincodeStubInterface, dataContractTypeID string, pageSize int32, bookmark string) pb.Response {
	queryResults, err := getDocumentsByDocTypeAndContractTypeWithPagination(stub, DATA_CONTRACT_DOCTYPE, dataContractTypeID, pageSize, bookmark)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}
	return shim.Success(queryResults)
}

func SelectBusinessDataSetsToUploadByContractType(stub shim.ChaincodeStubInterface, dataContractTypeID string) pb.Response {
	dataContractType, err := GetDataContractTypeStructState(stub, dataContractTypeID)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}
	provider := dataContractType.ProviderID
	queryResults, err := getDocumentsByDocTypeAndProviderAndFileStatusAndContractType(stub, DATA_CONTRACT_DOCTYPE, provider, "PROPOSAL", dataContractTypeID)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}
	return shim.Success(queryResults)
}

func SelectBusinessDataSetsToUploadByContractTypeWithPagination(stub shim.ChaincodeStubInterface, dataContractTypeID string, pageSize int32, bookmark string) pb.Response {
	dataContractType, err := GetDataContractTypeStructState(stub, dataContractTypeID)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}
	provider := dataContractType.ProviderID
	queryResults, err := getDocumentsByDocTypeAndProviderAndFileStatusAndContractTypeWithPagination(stub, DATA_CONTRACT_DOCTYPE, provider, "PROPOSAL", dataContractTypeID, pageSize, bookmark)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}
	return shim.Success(queryResults)
}

func SelectNumberOfBusinessDataSetsToUpload(stub shim.ChaincodeStubInterface, provider string) pb.Response {
	queryResults, err := getDocumentsByDocTypeAndProviderAndFileStatus(stub, DATA_CONTRACT_DOCTYPE, provider, "PROPOSAL")
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}
	dataContractArray := []DataContract{}
	err = json.Unmarshal(queryResults, &dataContractArray)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}
	responseMap := make(map[string]int32)
	for i := 0; i < len(dataContractArray); i++ {
		ok := responseMap[dataContractArray[i].DataContractTypeID]
		if ok != 0 {
			responseMap[dataContractArray[i].DataContractTypeID]++
		} else {
			responseMap[dataContractArray[i].DataContractTypeID] = 1
		}
	}
	responseMapAsBytes, err := json.Marshal(responseMap)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}
	return shim.Success(responseMapAsBytes)
}

func SelectBusinessDataSetsSoldShippedNotDownloaded(stub shim.ChaincodeStubInterface, providerId string, today string) pb.Response {
	query := "{\"selector\":{\"provider\":\"" + providerId + "\", \"$or\": [{ \"extras.fileStatus\":\"DATASHIPPED\" }, { \"extras.streamEndDateTime\":  {\"$gte\":\"" + today + "\" } }]}} "
	queryResults, err := getQueryResultForQueryString(stub, query)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}
	return shim.Success(queryResults)
}

func SelectBusinessDataSetsSoldShippedNotDownloadedWithPagination(stub shim.ChaincodeStubInterface, providerId string, today string, pageSize int32, bookmark string) pb.Response {
	query := "{\"selector\":{\"provider\":\"" + providerId + "\", \"$or\": [{ \"extras.fileStatus\":\"DATASHIPPED\" }, { \"extras.streamEndDateTime\":  {\"$gte\":\"" + today + "\" } }]}} "
	queryResults, err := getQueryResultForQueryStringWithPagination(stub, query, pageSize, bookmark)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}
	return shim.Success(queryResults)
}

func SelectBusinessDataSetsSoldAndDownloaded(stub shim.ChaincodeStubInterface, providerId string, today string) pb.Response {

	query := "{\"selector\":{\"provider\":\"" + providerId + "\", \"$or\": [{ \"extras.fileStatus\":\"DATARECEIVED\" }, { \"extras.streamEndDateTime\":  {\"$lt\":\"" + today + "\" } }]}} "
	queryResults, err := getQueryResultForQueryString(stub, query)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}
	return shim.Success(queryResults)
}

func SelectBusinessDataSetsSoldAndDownloadedWithPagination(stub shim.ChaincodeStubInterface, providerId string, today string, pageSize int32, bookmark string) pb.Response {

	query := "{\"selector\":{\"provider\":\"" + providerId + "\", \"$or\": [{ \"extras.fileStatus\":\"DATARECEIVED\" }, { \"extras.streamEndDateTime\":  {\"$lt\":\"" + today + "\" } }]}} "
	queryResults, err := getQueryResultForQueryStringWithPagination(stub, query, pageSize, bookmark)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}
	return shim.Success(queryResults)
}
func SelectBusinessDataSetsPurchasedNotUploaded(stub shim.ChaincodeStubInterface, consumer string) pb.Response {
	queryResults, err := getDocumentsByDocTypeAndConsumerAndFileStatus(stub, DATA_CONTRACT_DOCTYPE, consumer, "PROPOSAL")
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}
	return shim.Success(queryResults)
}

func SelectBusinessDataSetsPurchasedNotUploadedWithPagination(stub shim.ChaincodeStubInterface, consumer string, pageSize int32, bookmark string) pb.Response {
	queryResults, err := getDocumentsByDocTypeAndConsumerAndFileStatusWithPagination(stub, DATA_CONTRACT_DOCTYPE, consumer, "PROPOSAL", pageSize, bookmark)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}
	return shim.Success(queryResults)
}

func SelectBusinessDataSetsPurchasedUploadedNotDownloaded(stub shim.ChaincodeStubInterface, consumerId string, today string) pb.Response {

	query := "{\"selector\":{\"consumer\":\"" + consumerId + "\", \"$or\": [{ \"extras.fileStatus\":\"DATASHIPPED\" }, { \"extras.streamEndDateTime\":  {\"$gte\":\"" + today + "\" } }]} }"
	queryResults, err := getQueryResultForQueryString(stub, query)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}
	return shim.Success(queryResults)
}

func SelectBusinessDataSetsPurchasedUploadedNotDownloadedWithPagination(stub shim.ChaincodeStubInterface, consumerId string, today string, pageSize int32, bookmark string) pb.Response {

	query := "{\"selector\":{\"consumer\":\"" + consumerId + "\", \"$or\": [{ \"extras.fileStatus\":\"DATASHIPPED\" }, { \"extras.streamEndDateTime\":  {\"$gte\":\"" + today + "\" } }]}} "
	queryResults, err := getQueryResultForQueryStringWithPagination(stub, query, pageSize, bookmark)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}
	return shim.Success(queryResults)
}

func SelectBusinessDataSetsPurchasedDownloaded(stub shim.ChaincodeStubInterface, consumerId string, today string) pb.Response {

	query := "{\"selector\":{\"consumer\":\"" + consumerId + "\", \"$or\": [{ \"extras.fileStatus\":\"DATARECEIVED\" }, { \"extras.streamEndDateTime\":  {\"$lt\":\"" + today + "\" } }]}} "
	queryResults, err := getQueryResultForQueryString(stub, query)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}
	return shim.Success(queryResults)
}

func SelectBusinessDataSetsPurchasedDownloadedWithPagination(stub shim.ChaincodeStubInterface, consumerId string, today string, pageSize int32, bookmark string) pb.Response {

	query := "{\"selector\":{\"consumer\":\"" + consumerId + "\", \"$or\": [{ \"extras.fileStatus\":\"DATARECEIVED\" }, { \"extras.streamEndDateTime\":  {\"$lt\":\"" + today + "\" } }]}} "
	queryResults, err := getQueryResultForQueryStringWithPagination(stub, query, pageSize, bookmark)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}
	return shim.Success(queryResults)
}

func getDocumentsByDocTypeAndProvider(stub shim.ChaincodeStubInterface, docType string, providerId string) ([]byte, error) {
	query := "{\"selector\":{\"docType\":\"" + docType + "\", \"provider\":\"" + providerId + "\"}}"
	return getQueryResultForQueryString(stub, query)
}

func getDocumentsByDocTypeAndProviderWithPagination(stub shim.ChaincodeStubInterface, docType string, providerId string, pageSize int32, bookmark string) ([]byte, error) {
	query := "{\"selector\":{\"docType\":\"" + docType + "\", \"provider\":\"" + providerId + "\"}}"
	return getQueryResultForQueryStringWithPagination(stub, query, pageSize, bookmark)
}

func getDocumentsByDocTypeAndConsumer(stub shim.ChaincodeStubInterface, docType string, consumerId string) ([]byte, error) {
	query := "{\"selector\":{\"docType\":\"" + docType + "\", \"consumer\":\"" + consumerId + "\"}}"
	return getQueryResultForQueryString(stub, query)
}

func getDocumentsByDocTypeAndConsumerWithPagination(stub shim.ChaincodeStubInterface, docType string, consumerId string, pageSize int32, bookmark string) ([]byte, error) {
	query := "{\"selector\":{\"docType\":\"" + docType + "\", \"consumer\":\"" + consumerId + "\"}}"
	return getQueryResultForQueryStringWithPagination(stub, query, pageSize, bookmark)
}

func getDocumentsByDocTypeAndDataContractTypeAndFileStatus(stub shim.ChaincodeStubInterface, docType string, contractTypeId string, fileStatus string) ([]byte, error) {
	query := "{\"selector\":{\"docType\":\"" + docType + "\", \"dataContractType\":\"" + contractTypeId + "\", \"extras.fileStatus\":\"" + fileStatus + "\"}}"
	return getQueryResultForQueryString(stub, query)
}

func getDocumentsByDocTypeAndDataContractTypeAndFileStatusWithPagination(stub shim.ChaincodeStubInterface, docType string, dataContractType string, fileStatus string, pageSize int32, bookmark string) ([]byte, error) {
	query := "{\"selector\":{\"docType\":\"" + docType + "\", \"dataContractType\":\"" + dataContractType + "\", \"extras.fileStatus\":\"" + fileStatus + "\"}}"
	return getQueryResultForQueryStringWithPagination(stub, query, pageSize, bookmark)
}

func getDocumentsByDocTypeAndProviderAndFileStatus(stub shim.ChaincodeStubInterface, docType string, providerId string, fileStatus string) ([]byte, error) {
	query := "{\"selector\":{\"docType\":\"" + docType + "\", \"provider\":\"" + providerId + "\", \"extras.fileStatus\":\"" + fileStatus + "\"}}"
	return getQueryResultForQueryString(stub, query)
}

func getDocumentsByDocTypeAndProviderAndFileStatusWithPagination(stub shim.ChaincodeStubInterface, docType string, providerId string, fileStatus string, pageSize int32, bookmark string) ([]byte, error) {
	query := "{\"selector\":{\"docType\":\"" + docType + "\", \"provider\":\"" + providerId + "\", \"extras.fileStatus\":\"" + fileStatus + "\"}}"
	return getQueryResultForQueryStringWithPagination(stub, query, pageSize, bookmark)
}

func getDocumentsByDocTypeAndContractType(stub shim.ChaincodeStubInterface, docType string, dataContractType string) ([]byte, error) {
	query := "{\"selector\":{\"docType\":\"" + docType + "\", \"dataContractType\":\"" + dataContractType + "\"}}"
	return getQueryResultForQueryString(stub, query)
}

func getDocumentsByDocTypeAndContractTypeWithPagination(stub shim.ChaincodeStubInterface, docType string, dataContractType string, pageSize int32, bookmark string) ([]byte, error) {
	query := "{\"selector\":{\"docType\":\"" + docType + "\", \"dataContractType\":\"" + dataContractType + "\"}}"
	return getQueryResultForQueryStringWithPagination(stub, query, pageSize, bookmark)
}

func getDocumentsByDocTypeAndProviderAndFileStatusAndContractType(stub shim.ChaincodeStubInterface, docType string, providerId string, fileStatus string, dataContractType string) ([]byte, error) {
	query := "{\"selector\":{\"docType\":\"" + docType + "\", \"provider\":\"" + providerId + "\", \"extras.fileStatus\":\"" + fileStatus + "\", \"dataContractType\":\"" + dataContractType + "\"}}"
	return getQueryResultForQueryString(stub, query)
}

func getDocumentsByDocTypeAndProviderAndFileStatusAndContractTypeWithPagination(stub shim.ChaincodeStubInterface, docType string, providerId string, fileStatus string, dataContractType string, pageSize int32, bookmark string) ([]byte, error) {
	query := "{\"selector\":{\"docType\":\"" + docType + "\", \"provider\":\"" + providerId + "\", \"extras.fileStatus\":\"" + fileStatus + "\", \"dataContractType\":\"" + dataContractType + "\"}}"
	return getQueryResultForQueryStringWithPagination(stub, query, pageSize, bookmark)
}
func getDocumentsByDocTypeAndConsumerAndFileStatus(stub shim.ChaincodeStubInterface, docType string, consumerId string, fileStatus string) ([]byte, error) {
	query := "{\"selector\":{\"docType\":\"" + docType + "\", \"consumer\":\"" + consumerId + "\", \"extras.fileStatus\":\"" + fileStatus + "\"}}"
	return getQueryResultForQueryString(stub, query)
}

func getDocumentsByDocTypeAndConsumerAndFileStatusWithPagination(stub shim.ChaincodeStubInterface, docType string, consumerId string, fileStatus string, pageSize int32, bookmark string) ([]byte, error) {
	query := "{\"selector\":{\"docType\":\"" + docType + "\", \"consumer\":\"" + consumerId + "\", \"extras.fileStatus\":\"" + fileStatus + "\"}}"
	return getQueryResultForQueryStringWithPagination(stub, query, pageSize, bookmark)
}

// =========================================================================================
// Person related queries
// =========================================================================================

func GetPersons(stub shim.ChaincodeStubInterface) pb.Response {
	queryResults, err := getDocumentsByDocType(stub, PERSON_DOCTYPE)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}
	return shim.Success(queryResults)
}

func GetPersonsWithPagination(stub shim.ChaincodeStubInterface, pageSize int32, bookmark string) pb.Response {
	queryResults, err := getDocumentsByDocTypeWithPagination(stub, PERSON_DOCTYPE, pageSize, bookmark)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}
	return shim.Success(queryResults)
}

// =========================================================================================
// Review related queries
// =========================================================================================

func GetReviews(stub shim.ChaincodeStubInterface) pb.Response {
	queryResults, err := getDocumentsByDocType(stub, REVIEW_DOCTYPE)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}
	return shim.Success(queryResults)
}

func GetReviewsWithPagination(stub shim.ChaincodeStubInterface, pageSize int32, bookmark string) pb.Response {
	queryResults, err := getDocumentsByDocTypeWithPagination(stub, REVIEW_DOCTYPE, pageSize, bookmark)
	if err != nil {
		logger.Error(err.Error())
		return shim.Error(err.Error())
	}
	return shim.Success(queryResults)
}

// =========================================================================================
// getDocumentsByDocType functions
// =========================================================================================

func getDocumentsByDocType(stub shim.ChaincodeStubInterface, docType string) ([]byte, error) {
	query := "{\"selector\":{\"docType\":\"" + docType + "\"}}"
	return getQueryResultForQueryString(stub, query)
}

func getDocumentsByDocTypeWithPagination(stub shim.ChaincodeStubInterface, docType string, pageSize int32, bookmark string) ([]byte, error) {
	query := "{\"selector\":{\"docType\":\"" + docType + "\"}}"
	return getQueryResultForQueryStringWithPagination(stub, query, pageSize, bookmark)
}

// =========================================================================================
// getQueryResultForQueryString executes the passed in query string.
// Result set is built and returned as a byte array containing the JSON results.
// =========================================================================================
func getQueryResultForQueryString(stub shim.ChaincodeStubInterface, queryString string) ([]byte, error) {

	logInfo := "- getQueryResultForQueryString queryString:\n%s\n" + queryString
	logger.Info(logInfo)

	resultsIterator, err := stub.GetQueryResult(queryString)
	if err != nil {
		return nil, err
	}
	defer resultsIterator.Close()

	buffer, err := constructQueryResponseFromIterator(resultsIterator)
	if err != nil {
		return nil, err
	}

	logInfo = "- getQueryResultForQueryString queryResult:\n%s\n" + buffer.String()
	logger.Info(logInfo)

	return buffer.Bytes(), nil
}

// =========================================================================================
// getQueryResultForQueryStringWithPagination executes the passed in query string with
// pagination info. Result set is built and returned as a byte array containing the JSON results.
// =========================================================================================
func getQueryResultForQueryStringWithPagination(stub shim.ChaincodeStubInterface, queryString string, pageSize int32, bookmark string) ([]byte, error) {

	logger.Info("entering-getQueryResultForQueryStringWithPagination")
	defer logger.Info("exiting-getQueryResultForQueryStringWithPagination")

	resultsIterator, responseMetadata, err := stub.GetQueryResultWithPagination(queryString, pageSize, bookmark)
	if err != nil {
		return nil, err
	}
	defer resultsIterator.Close()

	buffer, err := constructQueryResponseFromIterator(resultsIterator)
	if err != nil {
		return nil, err
	}

	bufferWithPaginationInfo := addPaginationMetadataToQueryResults(buffer, responseMetadata)

	logInfo := "- getQueryResultForQueryString queryResult:\n%s\n" + bufferWithPaginationInfo.String()
	logger.Info(logInfo)

	return buffer.Bytes(), nil
}

// ===========================================================================================
// constructQueryResponseFromIterator constructs a JSON array containing query results from
// a given result iterator
// ===========================================================================================
func constructQueryResponseFromIterator(resultsIterator shim.StateQueryIteratorInterface) (*bytes.Buffer, error) {
	// buffer is a JSON array containing QueryResults
	var buffer bytes.Buffer
	buffer.WriteString("[")
	bArrayMemberAlreadyWritten := false
	for resultsIterator.HasNext() {
		queryResponse, err := resultsIterator.Next()
		if err != nil {
			return nil, err
		}
		if bArrayMemberAlreadyWritten == true {
			buffer.WriteString(",")
		}
		buffer.WriteString(string(queryResponse.Value))
		bArrayMemberAlreadyWritten = true
	}
	buffer.WriteString("]")
	return &buffer, nil
}

// ===========================================================================================
// addPaginationMetadataToQueryResults adds QueryResponseMetadata, which contains pagination
// info, to the constructed query results
// ===========================================================================================
func addPaginationMetadataToQueryResults(buffer *bytes.Buffer, responseMetadata *pb.QueryResponseMetadata) *bytes.Buffer {

	buffer.WriteString("[{\"ResponseMetadata\":{\"RecordsCount\":")
	buffer.WriteString("\"")
	buffer.WriteString(fmt.Sprintf("%v", responseMetadata.FetchedRecordsCount))
	buffer.WriteString("\"")
	buffer.WriteString(", \"Bookmark\":")
	buffer.WriteString("\"")
	buffer.WriteString(responseMetadata.Bookmark)
	buffer.WriteString("\"}}]")

	return buffer
}
