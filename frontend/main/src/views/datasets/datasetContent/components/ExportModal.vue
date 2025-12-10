<template>
  <div :class="`${prefixCls}`">
    <BasicModal
      v-bind="$attrs"
      @register="register"
      class="modal"
      :width="600"
      :title="t('common.exportText')"
      @ok="handleSubmit"
      @cancel="handleReset"
      ok-text="Export"
      :okButtonProps="{
        loading: isLoading,
      }"
    >
      <div class="content">
        <div class="flex items-center gap-10px">
          <div class="whitespace-nowrap" style="color: #333; width: 100px">Data</div>
          <Select v-model:value="dataType" style="width: 300px" allowClear>
            <Select.Option v-for="item in dataOption" :key="item.value" :value="item.value">
              {{ item.label }}
            </Select.Option>
          </Select>
        </div>
        <div class="flex items-center gap-10px">
          <div class="whitespace-nowrap" style="color: #333; width: 100px">Results</div>

          <TreeSelect
            :dropdownStyle="{ width: '200px' }"
            style="width: 300px"
            v-model:value="selectModelRunIds"
            :tree-data="props.modelRunResultList"
            tree-checkable
            :show-checked-strategy="SHOW_CHILD"
            allow-clear
            placeholder="Please select"
            showSearch
            treeDefaultExpandAll
          />
        </div>
        <div class="flex items-center gap-10px">
          <div class="whitespace-nowrap" style="color: #333; width: 100px">Export Format</div>
          <TreeSelect
            :dropdownStyle="{ width: '200px' }"
            style="width: 300px"
            v-model:value="dataFormat"
            :tree-data="dataFormatTreeData"
            allow-clear
            placeholder="Please select"
            showSearch
            treeDefaultExpandAll
          />

          <!-- <Select v-model:value="dataFormat" style="width: 300px" allowClear>
            <Select.Option v-for="item in dataFormatOption" :key="item.value" :value="item.value">
              {{ item.label }}
            </Select.Option>
          </Select> -->
        </div>
        <div class="flex items-center gap-8px">
          <Icon icon="eva:info-fill" size="20" color="#57CCEF" />
          <span style="color: #666"
            >Click
            <a href="https://docs.xtreme1.io/xtreme1-docs/export-data" target="_blank">here</a> to
            check out our data format explanation</span
          >
        </div>
      </div>
    </BasicModal>
  </div>
</template>
<script lang="ts" setup>
  import { defineEmits, ref, computed } from 'vue';
  import { useRoute } from 'vue-router';
  import { useI18n } from '/@/hooks/web/useI18n';
  import { useDesign } from '/@/hooks/web/useDesign';
  import { setEndTime, setStartTime } from '/@/utils/business/timeFormater';

  import { message, Select, TreeSelect } from 'ant-design-vue';
  import { BasicModal, useModalInner } from '/@/components/Modal';
  import { Icon } from '/@/components/Icon';

  const SHOW_CHILD = TreeSelect.SHOW_CHILD;

  import { exportData } from '/@/api/business/dataset';
  import { downloadOriginalZip } from '/@/api/business/dataset';
  import { dataTypeEnum } from '/@/api/business/model/datasetModel';

  const { query } = useRoute();
  const { id, dataId } = query;
  const { prefixCls } = useDesign('exportModal');
  const { t } = useI18n();
  const [register, { closeModal }] = useModalInner();

  const props = defineProps<{
    filterForm: any;
    modelRunResultList?: any;
    selectedList: string[];
    datasetType: string | undefined;
  }>();
  const emit = defineEmits(['setExportRecord']);

  const dataFormat = ref<string>('ORIGINAL_ZIP');

  let dataFormatOption = computed(() => {
    const originalZipOption = {
      value: 'ORIGINAL_ZIP',
      label: 'Original ZIP',
    };

    const xtreme1Options = [
      {
        value: 'XTREME1',
        label: 'Xtreme1',
      },
    ];
    const cocoOption = {
      value: 'COCO',
      label: 'COCO',
    };

    if (props.datasetType?.includes('LIDAR') || props.datasetType?.includes('TEXT')) {
      return [ originalZipOption ];
    } else {
      return [ originalZipOption, cocoOption];
    }
  });

  const dataFormatTreeData = computed(() => {
  return dataFormatOption.value.map(item => ({
    title: item.label,
    key: item.value,
    value: item.value,
    children: [],  // 자식 없으면 빈 배열
  }));
});

  enum dataOptionEmu {
    ALL = '',
    SELECTED = 'SELECTED',
    WITH_CURRENT_FLITER = 'WITH_CURRENT_FLITER',
  }
  const dataOption = [
    {
      value: dataOptionEmu.ALL,
      label: 'All',
    },
    {
      value: dataOptionEmu.SELECTED,
      label: 'Selected',
    },
    {
      value: dataOptionEmu.WITH_CURRENT_FLITER,
      label: 'With current filter',
    },
  ];

  let selectModelRunIds = ref<any>([]);
  let dataType = ref<string>('');

  const isLoading = ref<boolean>(false);
  const handleSubmit = async () => {
    const data = Object.assign(
      {
        datasetId: id,
      },
      {
        ...props.filterForm,
        createStartTime:
          props.filterForm.createStartTime && props.filterForm.createEndTime
            ? setStartTime(props.filterForm.createStartTime)
            : undefined,
        createEndTime:
          props.filterForm.createEndTime && props.filterForm.createStartTime
            ? setEndTime(props.filterForm.createEndTime)
            : undefined,
        projectIds: props.filterForm.projectIds && props.filterForm.projectIds + '',
        modelRunIds: props.filterForm.modelRunIds && props.filterForm.modelRunIds + '',
      },
    );
    if (data.type !== dataTypeEnum.SINGLE_DATA) {
      delete data.annotationCountMin;
      delete data.annotationCountMax;
    }
    if (data.type === dataTypeEnum.ALL) {
      data.type = undefined;
    }

    if (data.runRecordId && data.runRecordId.length) {
      data.runRecordId = data.runRecordId[1];
      data.minDataConfidence = data.confidenceSlider[0];
      data.maxDataConfidence = data.confidenceSlider[1];
    }
    delete data.confidenceSlider;
    console.log("handleSubmit 함수 실행됨"); 
    console.log("dataFormat.value: ", dataFormat.value);
    console.log("selectModelRunIds.value.toString(): ", selectModelRunIds.value.toString());
    console.log("selectModelRunIds.value?.length: ", selectModelRunIds.value?.length);
    if (dataFormat.value === 'ORIGINAL_ZIP') {
      try {
        isLoading.value = true;
        console.log("downloadOriginalZip api 호출"); 
        const presignedUrls = await downloadOriginalZip(id as string); // 리스트 반환
        console.log('presignedUrls:', presignedUrls);

        if (Array.isArray(presignedUrls) && presignedUrls.length > 0) {
          presignedUrls.forEach((url) => {
            console.log("url: ", url);
            window.open(url, '_blank');
          });
          message.success('원본 ZIP 다운로드가 시작');
        } else {
          message.warning('다운로드할 ZIP 파일이 없음');
        }

        closeModal();
      } catch (e) {
        message.error('원본 ZIP 다운로드 실패');
      } finally {
        isLoading.value = false;
      }
    }
    if (selectModelRunIds.value?.length){
      try {
        isLoading.value = true;
        const res = await exportData(fliterPa(data));
        message.success({
          content: 'export data succeeded',
          duration: 5,
        });
        emit('setExportRecord', res);
        closeModal();
      } catch (e) {}
    }
    setTimeout(() => {
      isLoading.value = false;
    }, 300);
  };
  let handleReset = () => {
    dataFormat.value = 'ORIGINAL_ZIP';
    dataType.value = '';
    selectModelRunIds.value = [];
  };

  let fliterPa = (data) => {
    let res: any = {};
    if (dataType.value === dataOptionEmu.ALL) {
      res.datasetId = data.datasetId;
    } else if (dataType.value === dataOptionEmu.SELECTED) {
      res.ids = props.selectedList.toString();
      res.datasetId = data.datasetId;
    } else if (dataType.value === dataOptionEmu.WITH_CURRENT_FLITER) {
      res = data;
      if (dataId) {
        res.ids = [dataId].toString();
      }
    }
    res.selectModelRunIds = selectModelRunIds.value
      .map((id) => {
        if (id === 'model--99') return -99;
        if (id === 'model--1') return -1;
        return id;
      })
      .filter((id) => !isNaN(Number(id)))
      .toString();
    res.dataFormat = dataFormat.value;
    return res;
  };
</script>
<style lang="less" scoped>
  .content {
    display: flex;
    flex-direction: column;
    gap: 20px;
    padding: 40px 50px;
    font-size: 14px;
    line-height: 16px;
  }
</style>
